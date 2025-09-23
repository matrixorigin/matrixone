#!/usr/bin/env python3
"""
Example demonstrating fulltext index functionality in MatrixOne Python SDK.

This example shows how to:
1. Create a table with text content
2. Create fulltext indexes
3. Insert data
4. Perform fulltext searches
5. Use different search modes and algorithms
"""

import os
from matrixone import Client, AsyncClient, FulltextAlgorithmType, FulltextModeType
from matrixone.sqlalchemy_ext import FulltextIndex, FulltextSearchBuilder


def sync_fulltext_example():
    """Synchronous fulltext index example"""
    print("=== Synchronous Fulltext Index Example ===")
    
    # Get connection parameters from environment
    host = os.getenv("MATRIXONE_HOST", "127.0.0.1")
    port = int(os.getenv("MATRIXONE_PORT", "6001"))
    user = os.getenv("MATRIXONE_USER", "root")
    password = os.getenv("MATRIXONE_PASSWORD", "111")
    database = os.getenv("MATRIXONE_DATABASE", "test")
    
    client = Client()
    
    try:
        # Connect to database
        client.connect(host=host, port=port, user=user, password=password, database=database)
        print(f"✓ Connected to MatrixOne at {host}:{port}")
        
        # Enable fulltext indexing
        client.execute("SET experimental_fulltext_index = 1")
        client.execute('SET ft_relevancy_algorithm = "BM25"')
        print("✓ Enabled fulltext indexing with BM25 algorithm")
        
        # Create a table for documents
        client.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                id BIGINT PRIMARY KEY,
                title VARCHAR(255),
                content TEXT,
                author VARCHAR(100)
            )
        """)
        print("✓ Created documents table")
        
        # Create fulltext index using client.fulltext_index
        client.fulltext_index.create(
            table_name="documents",
            name="ftidx_docs",
            columns=["title", "content"],
            algorithm=FulltextAlgorithmType.BM25
        )
        print("✓ Created fulltext index on title and content columns")
        
        # Insert sample documents
        documents = [
            (1, "Introduction to Python", "Python is a high-level programming language. It's great for beginners.", "Alice"),
            (2, "Machine Learning Basics", "Machine learning algorithms can learn from data automatically.", "Bob"),
            (3, "Database Design", "Good database design is crucial for application performance.", "Charlie"),
            (4, "Python Web Development", "Python can be used to build web applications with frameworks like Django.", "Alice"),
            (5, "Data Science with Python", "Python is widely used in data science for analysis and visualization.", "David"),
        ]
        
        for doc_id, title, content, author in documents:
            client.execute(
                "INSERT INTO documents (id, title, content, author) VALUES (?, ?, ?, ?)",
                (doc_id, title, content, author)
            )
        print(f"✓ Inserted {len(documents)} documents")
        
        # Perform fulltext searches
        print("\n--- Fulltext Search Results ---")
        
        # Search using natural language mode
        print("1. Natural Language Mode - Searching for 'Python':")
        result = client.execute("""
            SELECT id, title, MATCH(title, content) AGAINST('Python' IN NATURAL LANGUAGE MODE) as score
            FROM documents 
            WHERE MATCH(title, content) AGAINST('Python' IN NATURAL LANGUAGE MODE)
            ORDER BY score DESC
        """)
        
        for row in result.rows:
            print(f"   ID: {row[0]}, Title: {row[1]}, Score: {row[2]:.4f}")
        
        # Search using boolean mode
        print("\n2. Boolean Mode - Searching for '+Python +web':")
        result = client.execute("""
            SELECT id, title, content
            FROM documents 
            WHERE MATCH(title, content) AGAINST('+Python +web' IN BOOLEAN MODE)
        """)
        
        for row in result.rows:
            print(f"   ID: {row[0]}, Title: {row[1]}")
        
        # Search using the search builder
        print("\n3. Using FulltextSearchBuilder - Searching for 'machine learning':")
        search_builder = FulltextSearchBuilder("documents", ["title", "content"])
        search_builder.search("machine learning").with_score(True).limit(3)
        
        sql = search_builder.build_sql()
        print(f"   Generated SQL: {sql}")
        
        result = client.execute(sql)
        for row in result.rows:
            print(f"   ID: {row[0]}, Title: {row[1]}, Score: {row[-1]:.4f}")
        
        # Test different algorithms
        print("\n--- Testing Different Algorithms ---")
        
        # Switch to TF-IDF algorithm
        client.execute('SET ft_relevancy_algorithm = "TF-IDF"')
        print("✓ Switched to TF-IDF algorithm")
        
        result = client.execute("""
            SELECT id, title, MATCH(title, content) AGAINST('Python' IN NATURAL LANGUAGE MODE) as score
            FROM documents 
            WHERE MATCH(title, content) AGAINST('Python' IN NATURAL LANGUAGE MODE)
            ORDER BY score DESC
            LIMIT 3
        """)
        
        print("TF-IDF Results:")
        for row in result.rows:
            print(f"   ID: {row[0]}, Title: {row[1]}, Score: {row[2]:.4f}")
        
        # Drop the index
        client.fulltext_index.drop("documents", "ftidx_docs")
        print("✓ Dropped fulltext index")
        
    except Exception as e:
        print(f"✗ Error: {e}")
    finally:
        client.disconnect()
        print("✓ Disconnected from MatrixOne")


async def async_fulltext_example():
    """Asynchronous fulltext index example"""
    print("\n=== Asynchronous Fulltext Index Example ===")
    
    # Get connection parameters from environment
    host = os.getenv("MATRIXONE_HOST", "127.0.0.1")
    port = int(os.getenv("MATRIXONE_PORT", "6001"))
    user = os.getenv("MATRIXONE_USER", "root")
    password = os.getenv("MATRIXONE_PASSWORD", "111")
    database = os.getenv("MATRIXONE_DATABASE", "test")
    
    client = AsyncClient()
    
    try:
        # Connect to database
        await client.connect(host=host, port=port, user=user, password=password, database=database)
        print(f"✓ Connected to MatrixOne at {host}:{port}")
        
        # Enable fulltext indexing
        await client.execute("SET experimental_fulltext_index = 1")
        await client.execute('SET ft_relevancy_algorithm = "BM25"')
        print("✓ Enabled fulltext indexing with BM25 algorithm")
        
        # Create a table for articles
        await client.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                id BIGINT PRIMARY KEY,
                headline VARCHAR(255),
                body TEXT,
                category VARCHAR(50)
            )
        """)
        print("✓ Created articles table")
        
        # Create fulltext index using client.fulltext_index
        client.fulltext_index.create(
            table_name="articles",
            name="ftidx_articles",
            columns=["headline", "body"],
            algorithm=FulltextAlgorithmType.BM25
        )
        print("✓ Created fulltext index on headline and body columns")
        
        # Insert sample articles
        articles = [
            (1, "Tech News: AI Breakthrough", "Artificial intelligence has made significant progress in natural language processing.", "Technology"),
            (2, "Sports Update: Championship Results", "The championship finals concluded with an exciting victory for the home team.", "Sports"),
            (3, "Science Discovery: New Species Found", "Researchers have discovered a new species in the deep ocean ecosystem.", "Science"),
            (4, "Business Report: Market Analysis", "The stock market shows positive trends with technology stocks leading the way.", "Business"),
            (5, "Health News: Medical Research", "New medical research reveals promising treatments for chronic diseases.", "Health"),
        ]
        
        for article_id, headline, body, category in articles:
            await client.execute(
                "INSERT INTO articles (id, headline, body, category) VALUES (?, ?, ?, ?)",
                (article_id, headline, body, category)
            )
        print(f"✓ Inserted {len(articles)} articles")
        
        # Perform fulltext searches
        print("\n--- Async Fulltext Search Results ---")
        
        # Search for technology-related content
        print("1. Searching for 'technology':")
        result = await client.execute("""
            SELECT id, headline, MATCH(headline, body) AGAINST('technology' IN NATURAL LANGUAGE MODE) as score
            FROM articles 
            WHERE MATCH(headline, body) AGAINST('technology' IN NATURAL LANGUAGE MODE)
            ORDER BY score DESC
        """)
        
        for row in result.rows:
            print(f"   ID: {row[0]}, Headline: {row[1]}, Score: {row[2]:.4f}")
        
        # Search using boolean mode with multiple terms
        print("\n2. Boolean Mode - Searching for '+research +medical':")
        result = await client.execute("""
            SELECT id, headline, body
            FROM articles 
            WHERE MATCH(headline, body) AGAINST('+research +medical' IN BOOLEAN MODE)
        """)
        
        for row in result.rows:
            print(f"   ID: {row[0]}, Headline: {row[1]}")
        
        # Drop the index
        client.fulltext_index.drop("articles", "ftidx_articles")
        print("✓ Dropped fulltext index")
        
    except Exception as e:
        print(f"✗ Error: {e}")
    finally:
        await client.disconnect()
        print("✓ Disconnected from MatrixOne")


def transaction_fulltext_example():
    """Example of using fulltext indexes within transactions"""
    print("\n=== Transaction Fulltext Index Example ===")
    
    # Get connection parameters from environment
    host = os.getenv("MATRIXONE_HOST", "127.0.0.1")
    port = int(os.getenv("MATRIXONE_PORT", "6001"))
    user = os.getenv("MATRIXONE_USER", "root")
    password = os.getenv("MATRIXONE_PASSWORD", "111")
    database = os.getenv("MATRIXONE_DATABASE", "test")
    
    client = Client()
    
    try:
        # Connect to database
        client.connect(host=host, port=port, user=user, password=password, database=database)
        print(f"✓ Connected to MatrixOne at {host}:{port}")
        
        # Enable fulltext indexing
        client.execute("SET experimental_fulltext_index = 1")
        client.execute('SET ft_relevancy_algorithm = "BM25"')
        print("✓ Enabled fulltext indexing")
        
        # Use transaction for multiple operations
        with client.transaction() as tx:
            # Create table within transaction
            tx.execute("""
                CREATE TABLE IF NOT EXISTS books (
                    id BIGINT PRIMARY KEY,
                    title VARCHAR(255),
                    description TEXT,
                    author VARCHAR(100)
                )
            """)
            print("✓ Created books table within transaction")
            
            # Create fulltext index within transaction
            tx.fulltext_index.create(
                table_name="books",
                name="ftidx_books",
                columns=["title", "description"],
                algorithm=FulltextAlgorithmType.BM25
            )
            print("✓ Created fulltext index within transaction")
            
            # Insert data within transaction
            books = [
                (1, "The Python Handbook", "A comprehensive guide to Python programming for beginners and experts.", "John Doe"),
                (2, "Advanced Machine Learning", "Deep dive into machine learning algorithms and neural networks.", "Jane Smith"),
                (3, "Database Systems Design", "Complete guide to designing efficient database systems.", "Mike Johnson"),
            ]
            
            for book_id, title, description, author in books:
                tx.execute(
                    "INSERT INTO books (id, title, description, author) VALUES (?, ?, ?, ?)",
                    (book_id, title, description, author)
                )
            print(f"✓ Inserted {len(books)} books within transaction")
            
            # Search within transaction
            result = tx.execute("""
                SELECT id, title, MATCH(title, description) AGAINST('Python programming' IN NATURAL LANGUAGE MODE) as score
                FROM books 
                WHERE MATCH(title, description) AGAINST('Python programming' IN NATURAL LANGUAGE MODE)
                ORDER BY score DESC
            """)
            
            print("✓ Search results within transaction:")
            for row in result.rows:
                print(f"   ID: {row[0]}, Title: {row[1]}, Score: {row[2]:.4f}")
        
        print("✓ Transaction completed successfully")
        
        # Clean up
        client.fulltext_index.drop("books", "ftidx_books")
        print("✓ Dropped fulltext index")
        
    except Exception as e:
        print(f"✗ Error: {e}")
    finally:
        client.disconnect()
        print("✓ Disconnected from MatrixOne")


def main():
    """Main function to run all examples"""
    print("MatrixOne Fulltext Index Examples")
    print("=" * 50)
    
    # Run synchronous example
    sync_fulltext_example()
    
    # Run asynchronous example
    import asyncio
    asyncio.run(async_fulltext_example())
    
    # Run transaction example
    transaction_fulltext_example()
    
    print("\n" + "=" * 50)
    print("All examples completed!")


if __name__ == "__main__":
    main()
