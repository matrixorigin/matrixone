#!/usr/bin/env python3
"""
MatrixOne Fulltext Index Example

This example demonstrates how to use MatrixOne's fulltext indexing capabilities
with SQLAlchemy-style ORM models and API-based queries.
"""

import asyncio
import logging
import sys
import os
from datetime import datetime

# Add the matrixone module to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from matrixone import Client, AsyncClient, FulltextAlgorithmType, FulltextModeType
from matrixone.config import get_connection_params, print_config
from matrixone.logger import create_default_logger
from sqlalchemy import Column, Integer, String, Text, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base

# Create MatrixOne logger for all logging
logger = create_default_logger(
    enable_performance_logging=True,
    enable_sql_logging=True
)

# Define separate base classes for each example to avoid table conflicts
DocumentBase = declarative_base()
ArticleBase = declarative_base()
BookBase = declarative_base()

# Define models using SQLAlchemy-style syntax
class Document(DocumentBase):
    """Document model for fulltext search"""
    __tablename__ = "documents"
    
    id = Column(Integer, primary_key=True)
    title = Column(String(200), nullable=False)
    content = Column(Text, nullable=False)
    author = Column(String(100))
    created_at = Column(TIMESTAMP, default=datetime.now)

class Article(ArticleBase):
    """Article model for async fulltext search"""
    __tablename__ = "articles"
    
    id = Column(Integer, primary_key=True)
    headline = Column(String(200), nullable=False)
    body = Column(Text, nullable=False)
    category = Column(String(50))
    published_at = Column(TIMESTAMP, default=datetime.now)

class Book(BookBase):
    """Book model for transaction fulltext search"""
    __tablename__ = "books"
    
    id = Column(Integer, primary_key=True)
    title = Column(String(200), nullable=False)
    description = Column(Text, nullable=False)
    author = Column(String(100))
    isbn = Column(String(20))

def demo_sync_fulltext_index():
    """Demonstrate sync fulltext indexing"""
    logger.info("🚀 Starting sync fulltext index demo")
    
    # Print current configuration
    print_config()
    
    # Get connection parameters from config (supports environment variables)
    host, port, user, password, database = get_connection_params()
    
    try:
        # Create client
        client = Client()
        client.connect(host, port, user, password, database)
        
        logger.info("✅ Connected to MatrixOne")
        
        # Enable fulltext indexing using interface
        client.fulltext_index.enable_fulltext()
        client.execute('SET ft_relevancy_algorithm = "BM25"')
        logger.info("✅ Enabled fulltext indexing with BM25 algorithm")
        
        # Clean up existing table
        try:
            client.execute("DROP TABLE IF EXISTS documents")
        except:
            pass  # Table might not exist
        
        # Create table using ORM model
        client.create_all(DocumentBase)
        logger.info("✅ Created documents table using ORM")
        
        # Create fulltext index
        client.fulltext_index.create(
            table_name="documents",
            name="ftidx_docs",
            columns=["title", "content"],
            algorithm=FulltextAlgorithmType.BM25
        )
        logger.info("✅ Created fulltext index on title and content columns")
        
        # Insert sample data
        documents = [
            (1, "Introduction to Python", "Python is a powerful programming language used for web development, data science, and automation.", "John Doe"),
            (2, "Machine Learning Basics", "Machine learning is a subset of artificial intelligence that focuses on algorithms and statistical models.", "Jane Smith"),
            (3, "Database Design Principles", "Good database design is crucial for application performance and data integrity.", "Mike Johnson"),
            (4, "Python Web Development", "Learn how to build web applications using Python frameworks like Django and Flask.", "Sarah Wilson"),
            (5, "Data Science with Python", "Python provides excellent libraries for data analysis, visualization, and machine learning.", "David Brown"),
        ]
        
        for doc_id, title, content, author in documents:
            client.execute(
                f"INSERT INTO documents (id, title, content, author) VALUES ({doc_id}, '{title}', '{content}', '{author}')"
            )
        logger.info(f"✅ Inserted {len(documents)} documents")
        
        # Perform fulltext searches using ORM API
        logger.info("🔍 Fulltext Search Results:")
        
        # Search using natural language mode
        logger.info("1. Natural Language Mode - Searching for 'Python':")
        try:
            result = client.fulltext_index.fulltext_search(
                table_name="documents",
                columns=["title", "content"],
                search_term="Python",
                mode=FulltextModeType.NATURAL_LANGUAGE
            )
            
            for row in result.rows:
                try:
                    score = float(row[2]) if len(row) > 2 and row[2] is not None else 0.0
                    logger.info(f"   ID: {row[0]}, Title: {row[1]}, Score: {score:.4f}")
                except (ValueError, TypeError, IndexError):
                    logger.info(f"   ID: {row[0]}, Title: {row[1]}, Score: N/A")
        except Exception as e:
            logger.error(f"   Fulltext search error: {e}")
        
        # Search using boolean mode
        logger.info("2. Boolean Mode - Searching for '+Python +web':")
        try:
            result = client.fulltext_index.fulltext_search(
                table_name="documents",
                columns=["title", "content"],
                search_term="+Python +web",
                mode=FulltextModeType.BOOLEAN
            )
            
            for row in result.rows:
                logger.info(f"   ID: {row[0]}, Title: {row[1]}")
        except Exception as e:
            logger.error(f"   Boolean search error: {e}")
        
        # Search using fulltext index API
        logger.info("3. Fulltext Index API Search - Searching for 'machine learning':")
        try:
            result = client.fulltext_index.fulltext_search(
                table_name="documents",
                columns=["title", "content"],
                search_term="machine learning",
                mode=FulltextModeType.NATURAL_LANGUAGE
            )
            
            for row in result.rows:
                try:
                    score = float(row[2]) if len(row) > 2 and row[2] is not None else 0.0
                    logger.info(f"   ID: {row[0]}, Title: {row[1]}, Score: {score:.4f}")
                except (ValueError, TypeError, IndexError):
                    logger.info(f"   ID: {row[0]}, Title: {row[1]}, Score: N/A")
        except Exception as e:
            logger.error(f"   Fulltext index API search error: {e}")
        
        # Test different algorithms
        logger.info("🧪 Testing Different Algorithms:")
        
        # Switch to TF-IDF algorithm
        try:
            client.execute('SET ft_relevancy_algorithm = "TF-IDF"')
            logger.info("✅ Switched to TF-IDF algorithm")
            
            result = client.execute("""
                SELECT id, title, MATCH(title, content) AGAINST('Python' IN NATURAL LANGUAGE MODE) as score
                FROM documents 
                WHERE MATCH(title, content) AGAINST('Python' IN NATURAL LANGUAGE MODE)
                ORDER BY score DESC
                LIMIT 3
            """)
            
            logger.info("TF-IDF Results:")
            for row in result.rows:
                try:
                    score = float(row[2]) if row[2] is not None else 0.0
                    logger.info(f"   ID: {row[0]}, Title: {row[1]}, Score: {score:.4f}")
                except (ValueError, TypeError, IndexError):
                    logger.info(f"   ID: {row[0]}, Title: {row[1]}, Score: N/A")
        except Exception as e:
            logger.error(f"   TF-IDF algorithm test error: {e}")
            logger.info("   Note: Some algorithms may not be fully supported in this version")
        
        # Drop the index
        client.fulltext_index.drop("documents", "ftidx_docs")
        logger.info("✅ Dropped fulltext index")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
    finally:
        try:
            client.disconnect()
            logger.info("✅ Disconnected from MatrixOne")
        except:
            pass

async def demo_async_fulltext_index():
    """Demonstrate async fulltext indexing"""
    logger.info("🚀 Starting async fulltext index demo")
    
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    
    try:
        # Create async client
        client = AsyncClient()
        await client.connect(host, port, user, password, database)
        
        logger.info("✅ Connected to MatrixOne")
        
        # Enable fulltext indexing using interface
        await client.fulltext_index.enable_fulltext()
        await client.execute('SET ft_relevancy_algorithm = "BM25"')
        logger.info("✅ Enabled fulltext indexing with BM25 algorithm")
        
        # Clean up existing table
        try:
            await client.execute("DROP TABLE IF EXISTS articles")
        except:
            pass  # Table might not exist
        
        # Create table using ORM model
        await client.create_all(ArticleBase)
        logger.info("✅ Created articles table using ORM")
        
        # Create fulltext index
        await client.fulltext_index.create(
            table_name="articles",
            name="ftidx_articles",
            columns=["headline", "body"],
            algorithm=FulltextAlgorithmType.BM25
        )
        logger.info("✅ Created fulltext index on headline and body columns")
        
        # Insert sample data
        articles = [
            (1, "Tech News: AI Breakthrough", "Artificial intelligence researchers have made significant progress in natural language processing.", "Technology"),
            (2, "Sports Update: Championship Results", "The annual championship concluded with exciting matches and surprising outcomes.", "Sports"),
            (3, "Health Research: New Study Findings", "Medical researchers published findings that could lead to new treatment options.", "Health"),
            (4, "Business Report: Market Analysis", "Financial markets showed mixed results with technology stocks leading gains.", "Business"),
            (5, "Health News: Medical Research", "New research suggests potential breakthroughs in treating chronic diseases.", "Health"),
        ]
        
        for article_id, headline, body, category in articles:
            await client.execute(
                f"INSERT INTO articles (id, headline, body, category) VALUES ({article_id}, '{headline}', '{body}', '{category}')"
            )
        logger.info(f"✅ Inserted {len(articles)} articles")
        
        # Perform fulltext searches using ORM API
        logger.info("🔍 Async Fulltext Search Results:")
        
        # Search for technology-related content
        logger.info("1. Searching for 'technology':")
        try:
            result = await client.fulltext_index.fulltext_search(
                table_name="articles",
                columns=["headline", "body"],
                search_term="technology",
                mode=FulltextModeType.NATURAL_LANGUAGE
            )
            
            for row in result.rows:
                try:
                    score = float(row[2]) if len(row) > 2 and row[2] is not None else 0.0
                    logger.info(f"   ID: {row[0]}, Headline: {row[1]}, Score: {score:.4f}")
                except (ValueError, TypeError, IndexError):
                    logger.info(f"   ID: {row[0]}, Headline: {row[1]}, Score: N/A")
        except Exception as e:
            logger.error(f"   Fulltext search error: {e}")
        
        # Search using boolean mode with multiple terms
        logger.info("2. Boolean Mode - Searching for '+research +medical':")
        try:
            result = await client.fulltext_index.fulltext_search(
                table_name="articles",
                columns=["headline", "body"],
                search_term="+research +medical",
                mode=FulltextModeType.BOOLEAN
            )
            
            for row in result.rows:
                logger.info(f"   ID: {row[0]}, Headline: {row[1]}")
        except Exception as e:
            logger.error(f"   Boolean search error: {e}")
        
        # Drop the index
        await client.fulltext_index.drop("articles", "ftidx_articles")
        logger.info("✅ Dropped fulltext index")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
    finally:
        try:
            await client.disconnect()
            logger.info("✅ Disconnected from MatrixOne")
        except:
            pass

def demo_transaction_fulltext_index():
    """Demonstrate fulltext indexing within transactions"""
    logger.info("🚀 Starting transaction fulltext index demo")
    
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    
    try:
        # Create client
        client = Client()
        client.connect(host, port, user, password, database)
        
        logger.info("✅ Connected to MatrixOne")
        
        # Enable fulltext indexing using interface
        client.fulltext_index.enable_fulltext()
        logger.info("✅ Enabled fulltext indexing")
        
        # Clean up existing table before transaction
        client.drop_all(BookBase)
        
        # Create table using ORM
        client.create_all(BookBase)
        logger.info("✅ Created books table using ORM")

        # Use transaction for all operations
        with client.transaction() as tx:
            
            
            # Create fulltext index within transaction
            tx.fulltext_index.create(
                table_name="books",
                name="ftidx_books",
                columns=["title", "description"],
                algorithm=FulltextAlgorithmType.BM25
            )
            logger.info("✅ Created fulltext index within transaction")
            
            # Insert data within transaction
            books = [
                ("1", "The Python Handbook", "A comprehensive guide to Python programming for beginners and experts.", "John Doe"),
                ("2", "Advanced Machine Learning", "Deep dive into machine learning algorithms and neural networks.", "Jane Smith"),
                ("3", "Database Systems Design", "Complete guide to designing efficient database systems.", "Mike Johnson"),
            ]
            
            for book_id, title, description, author in books:
                tx.execute(
                    f"INSERT INTO books (id, title, description, author) VALUES ('{book_id}', '{title}', '{description}', '{author}')"
                )
            logger.info(f"✅ Inserted {len(books)} books within transaction")
            
            # Search within transaction using fulltext index API
            try:
                result = tx.fulltext_index.fulltext_search(
                    table_name="books",
                    columns=["title", "description"],
                    search_term="Python programming",
                    mode=FulltextModeType.NATURAL_LANGUAGE
                )
                
                logger.info("✅ Search results within transaction:")
                for row in result.rows:
                    try:
                        score = float(row[2]) if len(row) > 2 and row[2] is not None else 0.0
                        logger.info(f"   ID: {row[0]}, Title: {row[1]}, Score: {score:.4f}")
                    except (ValueError, TypeError, IndexError):
                        logger.info(f"   ID: {row[0]}, Title: {row[1]}, Score: N/A")
            except Exception as e:
                logger.error(f"   Transaction search error: {e}")
        
        logger.info("✅ Transaction completed successfully")
        
        # Clean up
        client.fulltext_index.drop("books", "ftidx_books")
        logger.info("✅ Dropped fulltext index")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
    finally:
        try:
            client.disconnect()
            logger.info("✅ Disconnected from MatrixOne")
        except:
            pass

def main():
    """Main function to run all examples"""
    print("MatrixOne Fulltext Index Examples")
    print("=" * 50)
    
    # Run sync example
    print("=== Synchronous Fulltext Index Example ===")
    demo_sync_fulltext_index()
    
    # Run async example
    print("\n=== Asynchronous Fulltext Index Example ===")
    asyncio.run(demo_async_fulltext_index())
    
    # Run transaction example
    print("\n=== Transaction Fulltext Index Example ===")
    demo_transaction_fulltext_index()
    
    print("\n" + "=" * 50)
    print("All examples completed!")

if __name__ == "__main__":
    main()