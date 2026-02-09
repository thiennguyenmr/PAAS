from pymilvus import connections, FieldSchema, CollectionSchema, DataType, Collection, utility
from spark_configs.settings import MILVUS_HOST, MILVUS_PORT

class MilvusConnector:
    @staticmethod
    def connect():
        """
        Connects to Milvus.
        """
        connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT)

    @staticmethod
    def create_collection(collection_name: str, dimension: int):
        """
        Creates a collection if it doesn't exist.
        """
        if not utility.has_collection(collection_name):
            fields = [
                FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=False),
                FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=dimension),
                FieldSchema(name="status", dtype=DataType.VARCHAR, max_length=200)
            ]
            schema = CollectionSchema(fields, "Hilo game embeddings")
            collection = Collection(collection_name, schema)
            print(f"Created collection {collection_name}")
            return collection
        else:
            return Collection(collection_name)

    @staticmethod
    def insert(collection, entities, batch_size=5000):
        """
        Inserts entities into a collection with batching for large datasets.

        Args:
            collection: The Milvus collection
            entities: List of entity lists [ids, vectors, metadata]
            batch_size: Number of entities to insert per batch (default: 5000)
        """
        # Get the total number of entities from the first list
        total_entities = len(entities[0]) if entities and len(entities) > 0 else 0

        if total_entities == 0:
            print("No entities to insert.")
            return

        # Insert in batches
        for i in range(0, total_entities, batch_size):
            end_idx = min(i + batch_size, total_entities)
            batch = [entity_list[i:end_idx] for entity_list in entities]
            collection.insert(batch)
            print(f"Inserted batch {i//batch_size + 1}: {end_idx - i} entities (total: {end_idx}/{total_entities})")

        # Flush after all batches
        collection.flush()
        print(f"Flushed all {total_entities} entities to Milvus.")
