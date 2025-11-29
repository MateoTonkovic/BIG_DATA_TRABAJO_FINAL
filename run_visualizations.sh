echo "=========================================="
echo "Generador de Visualizaciones - Productora"
echo "=========================================="
echo ""

# Verificar que Docker Compose esté corriendo
if ! docker ps | grep -q "spark-master"; then
    echo "Error: El contenedor spark-master no está corriendo."
    echo "   Por favor, ejecuta: docker-compose up -d"
    exit 1
fi

# Verificar que el pipeline se haya ejecutado
echo "📊 Verificando que el pipeline se haya ejecutado..."
if ! docker exec namenode hdfs dfs -test -d /curated/imdb 2>/dev/null; then
    echo "Advertencia: No se encontraron datos en la zona CURATED."
    echo "¿Deseas ejecutar el pipeline primero? (s/n)"
    read -r response
    if [[ "$response" =~ ^[Ss]$ ]]; then
        echo "Ejecutando pipeline..."
        docker cp imdb_pipeline.py spark-master:/tmp/imdb_pipeline.py
        docker exec spark-master /spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            /tmp/imdb_pipeline.py
        echo "Pipeline ejecutado."
    else
        echo "No se pueden generar visualizaciones sin datos."
        exit 1
    fi
fi

# Copiar script de visualizaciones
echo "📋 Copiando script de visualizaciones..."
docker cp visualizations.py spark-master:/tmp/visualizations.py

# Crear directorio de salida
echo "📁 Creando directorio de salida..."
docker exec spark-master mkdir -p /tmp/visualizations

# Ejecutar visualizaciones
echo "🎨 Generando visualizaciones..."
echo "   Esto puede tomar varios minutos..."
docker exec spark-master bash -c "PYSPARK_PYTHON=python3 /spark/bin/spark-submit \
    --master 'local[*]' \
    --driver-memory 4g \
    /tmp/visualizations.py"

# Verificar que se generaron las visualizaciones
echo ""
echo "🔍 Verificando archivos generados..."
files=(
    "01_estadisticas_por_genero.png"
    "02_rankings_popularidad.png"
    "03_analisis_por_decadas.png"
    "04_analisis_talento.png"
)

all_exist=true
for file in "${files[@]}"; do
    if docker exec spark-master test -f "/tmp/visualizations/$file"; then
        echo "   $file"
    else
        echo "   $file (no encontrado)"
        all_exist=false
    fi
done

if [ "$all_exist" = true ]; then
    # Crear directorio local de salida
    mkdir -p ./visualizations_output
    
    # Copiar archivos al host
    echo ""
    echo "📥 Copiando visualizaciones al host..."
    for file in "${files[@]}"; do
        docker cp "spark-master:/tmp/visualizations/$file" "./visualizations_output/$file"
        echo "   Copiado: ./visualizations_output/$file"
    done
    
    echo ""
    echo "=========================================="
    echo "Visualizaciones generadas"
    echo "=========================================="
    echo ""
    echo "Archivos disponibles en: ./visualizations_output/"
    echo ""
    echo "Visualizaciones generadas:"
    echo "  1. Estadísticas por género"
    echo "  2. Rankings de popularidad"
    echo "  3. Análisis por décadas"
    echo "  4. Análisis de talento"
    echo ""
else
    echo ""
    echo "Algunas visualizaciones no se generaron correctamente."
    echo "   Revisa los logs anteriores para más detalles."
    exit 1
fi

