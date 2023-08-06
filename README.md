# Pasos para ejecutar 
1. Posicionarse en la carpeta `entrega_3`. A esta altura debería ver el archivo `docker-compose.yml`.
2. Crear las siguientes carpetas a la misma altura del `docker-compose.yml`.
```bash
mkdir -p dags,logs,plugins,postgres_data,scripts
```
3. Crear un archivo con variables de entorno llamado `.env` ubicado a la misma altura que el `docker-compose.yml`. Cuyo contenido sea:
```bash
HOST=...
PORT=5439
DATABASE=...
USER=...
REDSHIFT_SCHEMA=...
PASSWORD=...
```
4. Crear la imagen de Airflow
```bash
cd entrega_3/docker_images/airflow
docker build . -f Dockerfile --tag airflow:airflow_2_6_2_laureaano
```
5. Si se desea generar las imagenes nuevamente, ejecutar los comandos que están en los Dockerfiles.
6. Ejecutar el siguiente comando para levantar los servicios de Airflow.
```bash
cd entrega_3
docker-compose up
```
7. Una vez que los servicios estén levantados, ingresar a Airflow en `http://localhost:8080/`.
8. Ejecutar el DAG `AlphaVantageETL`.