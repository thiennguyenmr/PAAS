from pyspark.sql.functions import col
from neo4j import GraphDatabase

from spark_modules.session_manager import get_spark_session
from spark_modules.minio_connector import MinIOConnector
from spark_modules.neo4j_connector import Neo4jConnector
from spark_configs.settings import NEO4J_URL, NEO4J_USER, NEO4J_PASSWORD


def create_indexes():
    """
    Create indexes in Neo4j BEFORE loading data.
    Without indexes, MATCH queries do full scans = extremely slow!
    """
    # Convert bolt:// URL to format neo4j driver expects
    driver = GraphDatabase.driver(NEO4J_URL, auth=(NEO4J_USER, NEO4J_PASSWORD))

    index_queries = [
        "CREATE INDEX player_user_id IF NOT EXISTS FOR (p:Player) ON (p.user_id)",
        "CREATE INDEX round_id IF NOT EXISTS FOR (r:GameRound) ON (r.round_id)",
        "CREATE INDEX session_id IF NOT EXISTS FOR (s:GameSession) ON (s.session_id)",
        "CREATE INDEX bet_type IF NOT EXISTS FOR (b:BetType) ON (b.bet_type)",
    ]

    with driver.session() as session:
        for query in index_queries:
            try:
                session.run(query)
                print(f"Index created: {query.split('IF NOT EXISTS')[0].strip()}")
            except Exception as e:
                print(f"Index may already exist: {e}")

    driver.close()
    print("Neo4j indexes ready.")


def main():
    spark = get_spark_session("05_Graph_Load_GameRound_Neo4j")

    input_path = "s3a://silver/hilo_game_rounds"
    df = MinIOConnector.read_parquet(spark, input_path)

    # Cache DataFrame to avoid re-reading from S3 multiple times
    df.cache()
    record_count = df.count()
    print(f"Loading {record_count} game round records into Neo4j graph database...")

    # CRITICAL: Create indexes BEFORE loading data
    print("Creating Neo4j indexes for fast lookups...")
    create_indexes()

    # Node 1: User (Player)
    if "user_id" in df.columns and "username" in df.columns:
        user_df = df.select("user_id", "username").distinct()
        Neo4jConnector.write_nodes(user_df, "Player", "user_id")
        print("Created Player nodes.")

    # Node 2: GameRound
    round_df = df.select(
        "round_id",
        "timestamp",
        "die1",
        "die2",
        "die3",
        "total_points",
        "outcome_type",
        "is_triple",
        "result"
    ).distinct()
    Neo4jConnector.write_nodes(round_df, "GameRound", "round_id")
    print("Created GameRound nodes.")

    # Node 3: Session
    if "session_id" in df.columns:
        session_df = df.select("session_id").distinct().filter(col("session_id").isNotNull())
        Neo4jConnector.write_nodes(session_df, "GameSession", "session_id")
        print("Created GameSession nodes.")

    # Node 4: BetType (different bet categories)
    bet_type_df = df.select("bet_type", "bet_category").distinct()
    Neo4jConnector.write_nodes(bet_type_df, "BetType", "bet_type")
    print("Created BetType nodes.")

    # Relationship 1: Player -[PLACED_BET]-> GameRound
    print("Creating PLACED_BET relationships...")
    placed_bet_query = """
    MATCH (p:Player {user_id: event.user_id})
    MATCH (r:GameRound {round_id: event.round_id})
    MERGE (p)-[rel:PLACED_BET {
        bet_amount: event.bet_amount,
        bet_type: event.bet_type,
        result: event.result,
        net_profit: event.net_profit,
        timestamp: event.timestamp
    }]->(r)
    """
    bet_rel_df = df.select("user_id", "round_id", "bet_amount", "bet_type", "result", "net_profit", "timestamp")
    Neo4jConnector.write_relationship(bet_rel_df, placed_bet_query)
    print("Created PLACED_BET relationships.")

    # Relationship 2: GameRound -[IN_SESSION]-> GameSession
    if "session_id" in df.columns:
        print("Creating IN_SESSION relationships...")
        session_query = """
        MATCH (r:GameRound {round_id: event.round_id})
        MATCH (s:GameSession {session_id: event.session_id})
        MERGE (r)-[:IN_SESSION]->(s)
        """
        session_rel_df = df.select("round_id", "session_id").distinct().filter(col("session_id").isNotNull())
        Neo4jConnector.write_relationship(session_rel_df, session_query)
        print("Created IN_SESSION relationships.")

    # Relationship 3: GameRound -[USED_BET_TYPE]-> BetType
    print("Creating USED_BET_TYPE relationships...")
    bet_type_query = """
    MATCH (r:GameRound {round_id: event.round_id})
    MATCH (b:BetType {bet_type: event.bet_type})
    MERGE (r)-[:USED_BET_TYPE]->(b)
    """
    bet_type_rel_df = df.select("round_id", "bet_type").distinct()
    Neo4jConnector.write_relationship(bet_type_rel_df, bet_type_query)
    print("Created USED_BET_TYPE relationships.")

    # Relationship 4: Player -[PARTICIPATED_IN]-> GameSession (aggregated)
    if "session_id" in df.columns:
        print("Creating PARTICIPATED_IN relationships...")
        participation_query = """
        MATCH (p:Player {user_id: event.user_id})
        MATCH (s:GameSession {session_id: event.session_id})
        MERGE (p)-[rel:PARTICIPATED_IN]->(s)
        ON CREATE SET rel.first_round = event.timestamp
        ON MATCH SET rel.last_round = event.timestamp
        """
        participation_df = df.select("user_id", "session_id", "timestamp").distinct().filter(col("session_id").isNotNull())
        Neo4jConnector.write_relationship(participation_df, participation_query)
        print("Created PARTICIPATED_IN relationships.")

    # Unpersist cached DataFrame
    df.unpersist()

    print(f"Game round graph loading completed! Processed {record_count} records.")
    spark.stop()

if __name__ == "__main__":
    main()