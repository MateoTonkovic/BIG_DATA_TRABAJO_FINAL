from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, when, isnan, isnull
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Configuración de estilo para las visualizaciones
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 10
sns.set_palette("husl")

# 1) Crear sesión Spark
spark = SparkSession.builder \
    .appName("IMDB Visualizations Productora") \
    .getOrCreate()

# 2) Rutas
hdfs = "hdfs://namenode:9000"
output_dir = "/tmp/visualizations"

curated = hdfs + "/curated/imdb"

# Crear directorio de salida si no existe
os.makedirs(output_dir, exist_ok=True)

print("=" * 80)
print("GENERANDO VISUALIZACIONES PARA PRODUCTORA CINEMATOGRÁFICA")
print("=" * 80)

# 1: Estadísticas por Género
print("\n[1/4] Generando estadísticas por género...")

genre_stats = spark.read.parquet(curated + "/genre_stats")
genre_stats_pd = genre_stats.orderBy(desc("title_count")).toPandas()

# Filtrar géneros nulos o vacíos
genre_stats_pd = genre_stats_pd[genre_stats_pd['genre'].notna()]
genre_stats_pd = genre_stats_pd[genre_stats_pd['genre'] != '']

# Top 15 géneros por cantidad de títulos
top_genres = genre_stats_pd.head(15)

fig, axes = plt.subplots(2, 2, figsize=(16, 12))
fig.suptitle('Análisis de Géneros Cinematográficos - KPIs para Productora', 
             fontsize=16, fontweight='bold', y=0.995)

# Gráfico 1: Cantidad de títulos por género
ax1 = axes[0, 0]
bars1 = ax1.barh(range(len(top_genres)), top_genres['title_count'].values, 
                color=sns.color_palette("husl", len(top_genres)))
ax1.set_yticks(range(len(top_genres)))
ax1.set_yticklabels(top_genres['genre'].values)
ax1.set_xlabel('Cantidad de Títulos', fontweight='bold')
ax1.set_title('Volumen de Producción por Género', fontweight='bold')
ax1.invert_yaxis()
ax1.grid(axis='x', alpha=0.3)

# Agregar valores en las barras
for i, v in enumerate(top_genres['title_count'].values):
    ax1.text(v + 100, i, f'{int(v):,}', va='center', fontweight='bold')

# Gráfico 2: Rating promedio por género
ax2 = axes[0, 1]
top_genres_rating = genre_stats_pd.nlargest(15, 'avg_rating')
bars2 = ax2.barh(range(len(top_genres_rating)), top_genres_rating['avg_rating'].values,
                color=sns.color_palette("coolwarm", len(top_genres_rating)))
ax2.set_yticks(range(len(top_genres_rating)))
ax2.set_yticklabels(top_genres_rating['genre'].values)
ax2.set_xlabel('Rating Promedio', fontweight='bold')
ax2.set_title('Calidad (Rating Promedio) por Género', fontweight='bold')
ax2.invert_yaxis()
ax2.grid(axis='x', alpha=0.3)
ax2.set_xlim([0, 10])

# Agregar valores en las barras
for i, v in enumerate(top_genres_rating['avg_rating'].values):
    ax2.text(v + 0.05, i, f'{v:.2f}', va='center', fontweight='bold')

# Gráfico 3: Duración promedio por género
ax3 = axes[1, 0]
top_genres_runtime = genre_stats_pd.nlargest(15, 'avg_runtime')
bars3 = ax3.barh(range(len(top_genres_runtime)), top_genres_runtime['avg_runtime'].values,
                color=sns.color_palette("viridis", len(top_genres_runtime)))
ax3.set_yticks(range(len(top_genres_runtime)))
ax3.set_yticklabels(top_genres_runtime['genre'].values)
ax3.set_xlabel('Duración Promedio (minutos)', fontweight='bold')
ax3.set_title('Duración Promedio por Género', fontweight='bold')
ax3.invert_yaxis()
ax3.grid(axis='x', alpha=0.3)

# Agregar valores en las barras
for i, v in enumerate(top_genres_runtime['avg_runtime'].values):
    if pd.notna(v):
        ax3.text(v + 2, i, f'{int(v)} min', va='center', fontweight='bold')

# Gráfico 4: Scatter - Volumen vs Rating
ax4 = axes[1, 1]
scatter = ax4.scatter(genre_stats_pd['title_count'], genre_stats_pd['avg_rating'],
                     s=genre_stats_pd['avg_votes']/100, alpha=0.6,
                     c=genre_stats_pd['avg_runtime'], cmap='viridis')
ax4.set_xlabel('Cantidad de Títulos (Volumen)', fontweight='bold')
ax4.set_ylabel('Rating Promedio (Calidad)', fontweight='bold')
ax4.set_title('Volumen vs Calidad por Género\n(Tamaño = Votos promedio)', fontweight='bold')
ax4.grid(alpha=0.3)
plt.colorbar(scatter, ax=ax4, label='Duración Promedio (min)')

plt.tight_layout()
plt.savefig(f'{output_dir}/01_estadisticas_por_genero.png', dpi=300, bbox_inches='tight')
print(f"   ✓ Guardado: {output_dir}/01_estadisticas_por_genero.png")
plt.close()

# 2: Rankings de Popularidad
print("\n[2/4] Generando rankings de popularidad...")

title_popularity = spark.read.parquet(curated + "/title_popularity")
# Filtrar solo películas (movies) y ordenar por popularidad
movies_popularity = title_popularity.filter(col("titleType") == "movie") \
    .orderBy(desc("popularity_score"))

top_movies = movies_popularity.limit(20).toPandas()

fig, axes = plt.subplots(1, 2, figsize=(16, 8))
fig.suptitle('Rankings de Popularidad - Identificación de Oportunidades de Mercado', 
             fontsize=16, fontweight='bold', y=0.995)

# Gráfico 1: Top 20 películas por popularidad
ax1 = axes[0]
bars1 = ax1.barh(range(len(top_movies)), top_movies['popularity_score'].values,
                color=sns.color_palette("rocket", len(top_movies)))
ax1.set_yticks(range(len(top_movies)))
# Truncar títulos largos para mejor visualización
titles_short = [t[:50] + '...' if len(t) > 50 else t for t in top_movies['primaryTitle'].values]
ax1.set_yticklabels(titles_short)
ax1.set_xlabel('Score de Popularidad (Votos × Rating)', fontweight='bold')
ax1.set_title('Top 20 Películas Más Populares', fontweight='bold')
ax1.invert_yaxis()
ax1.grid(axis='x', alpha=0.3)

# Agregar valores
for i, v in enumerate(top_movies['popularity_score'].values):
    ax1.text(v + max(top_movies['popularity_score']) * 0.01, i, 
            f'{v/1e6:.1f}M', va='center', fontweight='bold', fontsize=9)

# Gráfico 2: Distribución de popularidad vs rating
ax2 = axes[1]
movies_sample = movies_popularity.limit(1000).toPandas()
scatter2 = ax2.scatter(movies_sample['averageRating'], 
                      movies_sample['numVotes'],
                      c=movies_sample['popularity_score'],
                      s=50, alpha=0.5, cmap='plasma')
ax2.set_xlabel('Rating Promedio', fontweight='bold')
ax2.set_ylabel('Número de Votos (log scale)', fontweight='bold')
ax2.set_title('Distribución: Rating vs Votos\n(Color = Popularidad)', fontweight='bold')
ax2.set_yscale('log')
ax2.grid(alpha=0.3)
plt.colorbar(scatter2, ax=ax2, label='Score de Popularidad')

plt.tight_layout()
plt.savefig(f'{output_dir}/02_rankings_popularidad.png', dpi=300, bbox_inches='tight')
print(f"   ✓ Guardado: {output_dir}/02_rankings_popularidad.png")
plt.close()

# 3: Análisis por Décadas
print("\n[3/4] Generando análisis por décadas...")

title_by_decade = spark.read.parquet(curated + "/title_by_decade")
# Filtrar solo películas y décadas válidas
decade_stats = title_by_decade \
    .filter((col("titleType") == "movie") & 
            (col("decade").isNotNull()) & 
            (col("decade") >= 1900) & 
            (col("decade") <= 2020)) \
    .groupBy("decade") \
    .agg(
        count("*").alias("movie_count"),
        avg("averageRating").alias("avg_rating"),
        avg("numVotes").alias("avg_votes"),
        avg("runtimeMinutes").alias("avg_runtime")
    ) \
    .orderBy("decade")

decade_stats_pd = decade_stats.toPandas()

fig, axes = plt.subplots(2, 2, figsize=(16, 12))
fig.suptitle('Evolución Histórica del Cine - Análisis por Décadas', 
             fontsize=16, fontweight='bold', y=0.995)

# Gráfico 1: Cantidad de películas por década
ax1 = axes[0, 0]
ax1.plot(decade_stats_pd['decade'], decade_stats_pd['movie_count'], 
        marker='o', linewidth=2.5, markersize=8, color='#2E86AB')
ax1.fill_between(decade_stats_pd['decade'], decade_stats_pd['movie_count'], 
                alpha=0.3, color='#2E86AB')
ax1.set_xlabel('Década', fontweight='bold')
ax1.set_ylabel('Cantidad de Películas', fontweight='bold')
ax1.set_title('Volumen de Producción por Década', fontweight='bold')
ax1.grid(alpha=0.3)
ax1.set_xticks(decade_stats_pd['decade'][::2])

# Gráfico 2: Rating promedio por década
ax2 = axes[0, 1]
ax2.plot(decade_stats_pd['decade'], decade_stats_pd['avg_rating'], 
        marker='s', linewidth=2.5, markersize=8, color='#A23B72')
ax2.fill_between(decade_stats_pd['decade'], decade_stats_pd['avg_rating'], 
                alpha=0.3, color='#A23B72')
ax2.set_xlabel('Década', fontweight='bold')
ax2.set_ylabel('Rating Promedio', fontweight='bold')
ax2.set_title('Evolución de la Calidad (Rating) por Década', fontweight='bold')
ax2.grid(alpha=0.3)
ax2.set_ylim([0, 10])
ax2.set_xticks(decade_stats_pd['decade'][::2])

# Gráfico 3: Votos promedio por década
ax3 = axes[1, 0]
ax3.plot(decade_stats_pd['decade'], decade_stats_pd['avg_votes'], 
        marker='^', linewidth=2.5, markersize=8, color='#F18F01')
ax3.fill_between(decade_stats_pd['decade'], decade_stats_pd['avg_votes'], 
                alpha=0.3, color='#F18F01')
ax3.set_xlabel('Década', fontweight='bold')
ax3.set_ylabel('Votos Promedio (log scale)', fontweight='bold')
ax3.set_title('Evolución del Interés del Público', fontweight='bold')
ax3.set_yscale('log')
ax3.grid(alpha=0.3)
ax3.set_xticks(decade_stats_pd['decade'][::2])

# Gráfico 4: Duración promedio por década
ax4 = axes[1, 1]
ax4.plot(decade_stats_pd['decade'], decade_stats_pd['avg_runtime'], 
        marker='D', linewidth=2.5, markersize=8, color='#C73E1D')
ax4.fill_between(decade_stats_pd['decade'], decade_stats_pd['avg_runtime'], 
                alpha=0.3, color='#C73E1D')
ax4.set_xlabel('Década', fontweight='bold')
ax4.set_ylabel('Duración Promedio (minutos)', fontweight='bold')
ax4.set_title('Evolución de la Duración Promedio', fontweight='bold')
ax4.grid(alpha=0.3)
ax4.set_xticks(decade_stats_pd['decade'][::2])

plt.tight_layout()
plt.savefig(f'{output_dir}/03_analisis_por_decadas.png', dpi=300, bbox_inches='tight')
print(f"   ✓ Guardado: {output_dir}/03_analisis_por_decadas.png")
plt.close()

# 4: Resumen por Persona (Talent Analysis)
print("\n[4/4] Generando análisis de talento...")

person_rating = spark.read.parquet(curated + "/person_rating_summary")
# Filtrar personas con al menos 3 títulos y rating válido
person_stats = person_rating \
    .filter((col("title_count") >= 3) & 
            (col("avg_rating").isNotNull()) & 
            (col("primaryProfession").isNotNull())) \
    .orderBy(desc("avg_rating"))

# Top talento por profesión
professions = ['actor', 'actress', 'director', 'writer', 'producer']
fig, axes = plt.subplots(2, 3, figsize=(18, 12))
fig.suptitle('Análisis de Talento - KPIs para Casting y Selección de Equipos', 
             fontsize=16, fontweight='bold', y=0.995)

axes_flat = axes.flatten()

for idx, profession in enumerate(professions):
    ax = axes_flat[idx]
    
    # Filtrar por profesión (case insensitive)
    profession_data = person_stats \
        .filter(col("primaryProfession").contains(profession)) \
        .limit(15)
    
    if profession_data.count() > 0:
        profession_pd = profession_data.toPandas()
        
        bars = ax.barh(range(len(profession_pd)), profession_pd['avg_rating'].values,
                      color=sns.color_palette("Set2", len(profession_pd)))
        ax.set_yticks(range(len(profession_pd)))
        # Truncar nombres largos
        names_short = [n[:30] + '...' if len(n) > 30 else n 
                      for n in profession_pd['primaryName'].values]
        ax.set_yticklabels(names_short)
        ax.set_xlabel('Rating Promedio', fontweight='bold')
        ax.set_title(f'Top {profession.capitalize()}s por Rating', fontweight='bold')
        ax.invert_yaxis()
        ax.grid(axis='x', alpha=0.3)
        ax.set_xlim([0, 10])
        
        # Agregar valores y cantidad de títulos
        for i, (v, count) in enumerate(zip(profession_pd['avg_rating'].values, 
                                          profession_pd['title_count'].values)):
            ax.text(v + 0.1, i, f'{v:.2f} ({int(count)})', 
                   va='center', fontweight='bold', fontsize=8)
    else:
        ax.text(0.5, 0.5, f'No hay datos\npara {profession}', 
               ha='center', va='center', transform=ax.transAxes)
        ax.set_title(f'{profession.capitalize()}s', fontweight='bold')

# Gráfico adicional: Distribución general de talento
ax6 = axes_flat[5]
all_talent = person_stats.limit(100).toPandas()
scatter = ax6.scatter(all_talent['title_count'], all_talent['avg_rating'],
                     s=100, alpha=0.6, c=all_talent['title_count'], cmap='coolwarm')
ax6.set_xlabel('Cantidad de Títulos', fontweight='bold')
ax6.set_ylabel('Rating Promedio', fontweight='bold')
ax6.set_title('Distribución: Experiencia vs Calidad\n(Todos los profesionales)', fontweight='bold')
ax6.grid(alpha=0.3)
plt.colorbar(scatter, ax=ax6, label='Cantidad de Títulos')

plt.tight_layout()
plt.savefig(f'{output_dir}/04_analisis_talento.png', dpi=300, bbox_inches='tight')
print(f"   ✓ Guardado: {output_dir}/04_analisis_talento.png")
plt.close()

# 8) Resumen final
print("VISUALIZACIONES GENERADAS EXITOSAMENTE")
print("=" * 80)
print(f"\nArchivos guardados en: {output_dir}/")
print("\n1. 01_estadisticas_por_genero.png - Análisis de géneros cinematográficos")
print("2. 02_rankings_popularidad.png - Rankings y oportunidades de mercado")
print("3. 03_analisis_por_decadas.png - Evolución histórica del cine")
print("4. 04_analisis_talento.png - Análisis de talento para casting")

spark.stop()

