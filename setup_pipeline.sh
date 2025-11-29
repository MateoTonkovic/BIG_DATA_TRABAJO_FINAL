set -e

echo "Setup Pipeline - Productora Cinematográfica"

# Verificar que Docker Compose esté corriendo
echo "🔍 Verificando que Docker Compose esté corriendo..."
if ! docker ps | grep -q "namenode"; then
    echo "Error: Los contenedores no están corriendo."
    echo "   Por favor, ejecuta primero: docker-compose up -d"
    exit 1
fi
echo " Contenedores corriendo"

# Esperar a que HDFS esté listo
echo ""
echo "⏳ Esperando a que HDFS esté listo..."
max_attempts=30
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if docker exec namenode hdfs dfsadmin -report 2>/dev/null | grep -q "Live datanodes"; then
        echo " HDFS está listo"
        break
    fi
    attempt=$((attempt + 1))
    echo "   Intento $attempt/$max_attempts..."
    sleep 2
done

if [ $attempt -eq $max_attempts ]; then
    echo "HDFS puede no estar completamente listo, pero continuando..."
fi

# Verificar que existan los archivos de datos
echo ""
echo "📁 Verificando archivos de datos..."
data_files=("data/title.basics.tsv" "data/title.ratings.tsv" "data/name.basics.tsv")
all_exist=true

for file in "${data_files[@]}"; do
    if [ -f "$file" ]; then
        echo " $file"
    else
        echo "$file (no encontrado)"
        all_exist=false
    fi
done

if [ "$all_exist" = false ]; then
    echo ""
    echo "Error: Faltan archivos de datos en el directorio data/"
    echo "   Asegúrate de tener:"
    echo "   - data/title.basics.tsv"
    echo "   - data/title.ratings.tsv"
    echo "   - data/name.basics.tsv"
    exit 1
fi

# Copiar datos al contenedor namenode
echo ""
echo "📤 Copiando datos al contenedor namenode..."
docker cp data/ namenode:/tmp/data_imdb
echo " Datos copiados"

# Crear directorios en HDFS
echo ""
echo "📂 Creando estructura de directorios en HDFS..."
docker exec namenode hdfs dfs -mkdir -p /landing/imdb
docker exec namenode hdfs dfs -mkdir -p /raw/imdb
docker exec namenode hdfs dfs -mkdir -p /curated/imdb
echo " Directorios creados:"
echo "      - /landing/imdb (zona de ingesta)"
echo "      - /raw/imdb (zona raw)"
echo "      - /curated/imdb (zona curada)"

# Cargar datos a HDFS
echo ""
echo "⬆️  Cargando datos a HDFS (esto puede tomar unos minutos)..."
# Cargar cada archivo individualmente para evitar problemas con wildcards
for file in title.basics.tsv title.ratings.tsv name.basics.tsv; do
    if docker exec namenode test -f "/tmp/data_imdb/$file"; then
        echo "   Cargando $file..."
        docker exec namenode hdfs dfs -put "/tmp/data_imdb/$file" "/landing/imdb/$file"
    else
        echo "Archivo $file no encontrado en /tmp/data_imdb/"
    fi
done
echo " Datos cargados a HDFS"

# Verificar que los datos estén en HDFS
echo ""
echo "🔍 Verificando datos en HDFS..."
hdfs_files=$(docker exec namenode hdfs dfs -ls /landing/imdb/ | grep -E "\.tsv$" | wc -l)
if [ "$hdfs_files" -ge 3 ]; then
    echo " $hdfs_files archivos encontrados en /landing/imdb/"
    docker exec namenode hdfs dfs -ls /landing/imdb/ | grep "\.tsv"
else
    echo "Advertencia: Puede que no todos los archivos se hayan cargado correctamente"
fi

# Verificar que Spark esté listo
echo ""
echo "🔍 Verificando que Spark esté listo..."
if docker ps | grep -q "spark-master"; then
    echo " Spark Master está corriendo"
    # Intentar conectar
    if docker exec spark-master /spark/bin/spark-submit --version > /dev/null 2>&1; then
        echo " Spark está operativo"
    else
        echo "     Spark puede no estar completamente listo, pero debería funcionar"
    fi
else
    echo "Advertencia: Spark Master no está corriendo"
    echo "   Ejecuta: docker-compose up -d"
fi

echo ""
echo "=========================================="
echo " Setup completado exitosamente!"
echo "=========================================="
echo ""

