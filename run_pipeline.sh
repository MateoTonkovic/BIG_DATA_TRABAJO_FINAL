set -e

echo "Ejecutando Pipeline - Productora Cinematográfica"

# Verificar que los datos estén en HDFS
echo "🔍 Verificando que los datos estén en HDFS..."
if ! docker exec namenode hdfs dfs -test -d /landing/imdb 2>/dev/null; then
    echo "Error: Los datos no están en HDFS."
    echo "Por favor, ejecuta primero: ./setup_pipeline.sh"
    exit 1
fi

file_count=$(docker exec namenode hdfs dfs -ls /landing/imdb/ 2>/dev/null | grep -E "\.tsv$" | wc -l)
if [ "$file_count" -lt 3 ]; then
    echo "Error: Faltan archivos en /landing/imdb/"
    echo "Se encontraron solo $file_count archivos. Se esperan al menos 3."
    echo "Por favor, ejecuta: ./setup_pipeline.sh"
    exit 1
fi

echo "    $file_count archivos encontrados en /landing/imdb/"

# Verificar que Spark esté corriendo
if ! docker ps | grep -q "spark-master"; then
    echo "Error: Spark Master no está corriendo."
    echo "Por favor, ejecuta: docker-compose up -d"
    exit 1
fi

# Copiar pipeline al contenedor
echo ""
echo "📋 Copiando pipeline al contenedor Spark..."
docker cp imdb_pipeline.py spark-master:/tmp/imdb_pipeline.py
echo "Pipeline copiado"

# Ejecutar pipeline
echo ""
echo "Ejecutando pipeline (esto puede tomar varios minutos)..."
echo "Procesando datos de IMDb..."
echo ""

docker exec spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --driver-memory 2g \
    --executor-memory 2g \
    /tmp/imdb_pipeline.py

# Verificar resultados
echo ""
echo "Verificando resultados..."
echo ""

# Verificar zona RAW
raw_count=$(docker exec namenode hdfs dfs -ls /raw/imdb/ 2>/dev/null | grep "^d" | wc -l)
if [ "$raw_count" -ge 3 ]; then
    echo "Zona RAW: $raw_count datasets creados"
    docker exec namenode hdfs dfs -ls /raw/imdb/ | grep "^d" | awk '{print "      - " $NF}'
else
    echo "Zona RAW: Solo $raw_count datasets encontrados (esperados al menos 3)"
fi

# Verificar zona CURATED
curated_count=$(docker exec namenode hdfs dfs -ls /curated/imdb/ 2>/dev/null | grep "^d" | wc -l)
if [ "$curated_count" -ge 5 ]; then
    echo "    Zona CURATED: $curated_count datasets creados"
    docker exec namenode hdfs dfs -ls /curated/imdb/ | grep "^d" | awk '{print "      - " $NF}'
else
    echo "Zona CURATED: Solo $curated_count datasets encontrados (esperados al menos 5)"
fi

echo ""
echo "=========================================="
echo " Pipeline ejecutado exitosamente!"
echo "=========================================="
echo ""

