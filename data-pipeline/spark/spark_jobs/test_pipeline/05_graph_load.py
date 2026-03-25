from spark_modules.session_manager import get_spark_session
from spark_modules.minio_connector import MinIOConnector
from spark_modules.neo4j_connector import Neo4jConnector

def main():
    spark = get_spark_session("05_Graph_Load_Neo4j")

    input_path = "s3a://silver/hilo_game_data"
    df = MinIOConnector.read_parquet(spark, input_path)

    # Nodes: User
    if "user_id" in df.columns:
        user_df = df.select("user_id").distinct()
        Neo4jConnector.write_nodes(user_df, "User", "user_id")
        print("Wrote User nodes.")

    # Nodes: Game
    game_df = df.select("id", "status").distinct()
    Neo4jConnector.write_nodes(game_df, "Game", "id")
    print("Wrote Game nodes.")

    # Relationship: PLAYED
    query = "MATCH (u:User {user_id: event.user_id}), (g:Game {id: event.id}) MERGE (u)-[:PLAYED]->(g)"
    Neo4jConnector.write_relationship(df.select("user_id", "id"), query)
    print("Wrote PLAYED relationships.")

    spark.stop()

if __name__ == "__main__":
    main()
