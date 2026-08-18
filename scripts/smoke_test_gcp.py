"""Smoke test: confirma que a service account consegue acessar o dataset do
BigQuery e o bucket do GCS designados, antes de rodar o pipeline completo."""
from dagster_project.resources.gcp import BigQueryResource, GCSResource

bq = BigQueryResource()
gcs = GCSResource()

client = bq.get_client()
dataset_ref = f"{bq.project}.{bq.dataset}"
dataset = client.get_dataset(dataset_ref)
print(f"OK BigQuery: dataset acessível -> {dataset_ref} (location={dataset.location})")

bucket = gcs.get_bucket()

# a SA tem só roles/storage.objectAdmin (escopo mínimo do enunciado), que não
# inclui storage.buckets.get — por isso não chamamos bucket.reload() aqui,
# validamos diretamente a permissão real que importa: escrita de objetos.
blob = bucket.blob("_smoke_test/ping.txt")
blob.upload_from_string("ping")
blob.delete()
print("OK GCS: escrita e exclusão de objeto confirmadas")
