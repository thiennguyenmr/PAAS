from spark_modules.session_manager import get_spark_session
from spark_modules.minio_connector import MinIOConnector
from spark_modules.neo4j_connector import Neo4jConnector
from pyspark.sql.functions import col

def main():
    spark = get_spark_session("05_Graph_Load_GameChat_Neo4j")

    input_path = "s3a://silver/chat_messages"
    df = MinIOConnector.read_parquet(spark, input_path)

    print("Loading chat messages into Neo4j graph database...")

    # Node 1: User (includes both players and system users)
    if "user_id" in df.columns and "username" in df.columns:
        user_df = df.select("user_id", "username").distinct()
        Neo4jConnector.write_nodes(user_df, "ChatUser", "user_id")
        print(f"Created {user_df.count()} ChatUser nodes.")

    # Node 2: Message
    message_df = df.select(
        "message_id",
        "message_content",
        "message_type",
        "timestamp",
        "status",
        "language",
        "message_length",
        "word_count",
        "has_positive_sentiment",
        "has_negative_sentiment"
    ).distinct()
    Neo4jConnector.write_nodes(message_df, "Message", "message_id")
    print(f"Created {message_df.count()} Message nodes.")

    # Node 3: Channel
    if "channel" in df.columns:
        channel_df = df.select("channel").distinct()
        Neo4jConnector.write_nodes(channel_df, "Channel", "channel")
        print(f"Created {channel_df.count()} Channel nodes.")

    # Node 4: GameSession (if session_id exists)
    if "session_id" in df.columns:
        session_df = df.select("session_id").distinct().filter(col("session_id").isNotNull())
        Neo4jConnector.write_nodes(session_df, "ChatSession", "session_id")
        print(f"Created {session_df.count()} ChatSession nodes.")

    # Relationship 1: User -[SENT]-> Message
    print("Creating SENT relationships...")
    sent_query = """
    MATCH (u:ChatUser {user_id: event.user_id})
    MATCH (m:Message {message_id: event.message_id})
    MERGE (u)-[rel:SENT {
        timestamp: event.timestamp,
        message_type: event.message_type
    }]->(m)
    """
    sent_rel_df = df.select("user_id", "message_id", "timestamp", "message_type")
    Neo4jConnector.write_relationship(sent_rel_df, sent_query)
    print("Created SENT relationships.")

    # Relationship 2: Message -[POSTED_IN]-> Channel
    if "channel" in df.columns:
        print("Creating POSTED_IN relationships...")
        posted_query = """
        MATCH (m:Message {message_id: event.message_id})
        MATCH (c:Channel {channel: event.channel})
        MERGE (m)-[:POSTED_IN]->(c)
        """
        posted_rel_df = df.select("message_id", "channel")
        Neo4jConnector.write_relationship(posted_rel_df, posted_query)
        print("Created POSTED_IN relationships.")

    # Relationship 3: User -[REPLIED_TO]-> User (for private messages)
    if "recipient_id" in df.columns:
        print("Creating REPLIED_TO relationships for private messages...")
        private_df = df.filter(
            (col("message_type") == "private") &
            col("recipient_id").isNotNull()
        )

        if private_df.count() > 0:
            reply_query = """
            MATCH (sender:ChatUser {user_id: event.user_id})
            MATCH (recipient:ChatUser {user_id: event.recipient_id})
            MERGE (sender)-[rel:REPLIED_TO]->(recipient)
            ON CREATE SET rel.message_count = 1, rel.first_message = event.timestamp
            ON MATCH SET rel.message_count = rel.message_count + 1, rel.last_message = event.timestamp
            """
            reply_rel_df = private_df.select("user_id", "recipient_id", "timestamp")
            Neo4jConnector.write_relationship(reply_rel_df, reply_query)
            print("Created REPLIED_TO relationships.")
        else:
            print("No private messages found for REPLIED_TO relationships.")

    # Relationship 4: Message -[PART_OF]-> ChatSession (if session exists)
    if "session_id" in df.columns:
        print("Creating PART_OF relationships...")
        session_msg_df = df.filter(col("session_id").isNotNull())

        if session_msg_df.count() > 0:
            session_query = """
            MATCH (m:Message {message_id: event.message_id})
            MATCH (s:ChatSession {session_id: event.session_id})
            MERGE (m)-[:PART_OF]->(s)
            """
            session_rel_df = session_msg_df.select("message_id", "session_id")
            Neo4jConnector.write_relationship(session_rel_df, session_query)
            print("Created PART_OF relationships.")
        else:
            print("No session data found for PART_OF relationships.")

    # Relationship 5: ChatUser -[ACTIVE_IN]-> Channel (user activity per channel)
    if "channel" in df.columns:
        print("Creating ACTIVE_IN relationships...")
        active_query = """
        MATCH (u:ChatUser {user_id: event.user_id})
        MATCH (c:Channel {channel: event.channel})
        MERGE (u)-[rel:ACTIVE_IN]->(c)
        ON CREATE SET rel.message_count = 1, rel.first_activity = event.timestamp
        ON MATCH SET rel.message_count = rel.message_count + 1, rel.last_activity = event.timestamp
        """
        active_rel_df = df.select("user_id", "channel", "timestamp")
        Neo4jConnector.write_relationship(active_rel_df, active_query)
        print("Created ACTIVE_IN relationships.")

    print("Chat message graph loading completed!")
    print("\nGraph Schema Summary:")
    print("  Nodes: ChatUser, Message, Channel, ChatSession")
    print("  Relationships:")
    print("    - (ChatUser)-[SENT]->(Message)")
    print("    - (Message)-[POSTED_IN]->(Channel)")
    print("    - (ChatUser)-[REPLIED_TO]->(ChatUser)")
    print("    - (Message)-[PART_OF]->(ChatSession)")
    print("    - (ChatUser)-[ACTIVE_IN]->(Channel)")

    spark.stop()

if __name__ == "__main__":
    main()