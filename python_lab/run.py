from matrixone import Client
try:
    from sqlalchemy.orm import declarative_base
except ImportError:
    from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import Column, Integer, String, Text
from matrixone.sqlalchemy_ext import create_vector_column
from sqlalchemy import text
def get_client():
  client = Client()
  client.connect(host='localhost', port=6001, user='root', password='111', database='test')
  return client


if __name__ == "__main__":
  client = get_client()

  base = declarative_base()
  class Embbeding(base):
    __tablename__ = "embeddings"
    vector_id = Column(String(16), primary_key=True)
    embedding = create_vector_column(16, "f32")
    description = Column(String(100))

  class FulltextExample(base):
    __tablename__ = "fulltext_examples"
    id = Column(Integer, primary_key=True)
    title = Column(String(200))
    content = Column(Text)
    description = Column(String(500))

  client.drop_all(base)
  client.create_all(base)
  # mock 5 rows in a transaction


  client.vector_ops.batch_insert("embeddings", [
    {"vector_id": "1", "embedding": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6], "description": "Machine learning tutorial for beginners"},
    {"vector_id": "2", "embedding": [0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7], "description": "Advanced deep learning with neural networks"},
    {"vector_id": "3", "embedding": [0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8], "description": "Natural language processing techniques"},
    {"vector_id": "4", "embedding": [0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9], "description": "Computer vision and image recognition"},
    {"vector_id": "5", "embedding": [0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0], "description": "Data science and statistical analysis"},
    {"vector_id": "6", "embedding": [0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1], "description": "Blockchain technology and cryptocurrencies"},
    {"vector_id": "7", "embedding": [0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2], "description": "Cloud computing and distributed systems"},
    {"vector_id": "8", "embedding": [0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3], "description": "Cybersecurity and information protection"},
    {"vector_id": "9", "embedding": [0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4], "description": "Mobile app development with React Native"},
    {"vector_id": "10", "embedding": [1.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2, 0.3, 0.4, 0.5], "description": "Web development with modern frameworks"},
  ])

  from matrixone.sqlalchemy_ext import VectorOpType

  client.vector_ops.enable_ivf()
  client.vector_ops.create_ivf("embeddings", "idx_embedding", "embedding", 100, VectorOpType.VECTOR_L2_OPS)

  index = client.get_pinecone_index("embeddings", "embedding")
  filter = {
    "description": {
      "$in": ["Machine learning tutorial for beginners", "Advanced deep learning with neural networks"]
    }
  }
  # Use a more realistic query vector (similar to machine learning content)
  query_vector = [0.15, 0.25, 0.35, 0.45, 0.55, 0.65, 0.75, 0.85, 0.95, 0.05, 0.15, 0.25, 0.35, 0.45, 0.55, 0.65]
  results = index.query(query_vector, top_k=5, include_metadata=True, include_values=False, filter=filter)
  print("Vector Search Results:")
  print(f"Found {len(results.matches)} matches")
  for match in results.matches:
    print(f"  ID: {match.id}, Score: {match.score:.4f}, Description: {match.metadata['description']}")
  print()


  client.vector_ops.batch_insert("fulltext_examples", [
    {"id": "1", "title": "Introduction to Machine Learning", "content": "Machine learning is a subset of artificial intelligence that focuses on algorithms and statistical models. It enables computers to learn and make decisions from data without being explicitly programmed.", "description": "A comprehensive guide to understanding machine learning concepts and applications"},
    {"id": "2", "title": "Deep Learning Fundamentals", "content": "Deep learning uses neural networks with multiple layers to model and understand complex patterns in data. It has revolutionized fields like computer vision, natural language processing, and speech recognition.", "description": "Explore the foundations of deep learning and neural network architectures"},
    {"id": "3", "title": "Natural Language Processing", "content": "NLP combines computational linguistics with machine learning to help computers understand, interpret, and manipulate human language. It powers applications like chatbots, translation services, and sentiment analysis.", "description": "Learn how computers process and understand human language"},
    {"id": "4", "title": "Computer Vision Applications", "content": "Computer vision enables machines to interpret and understand visual information from the world. It is used in autonomous vehicles, medical imaging, facial recognition, and augmented reality applications.", "description": "Discover how computers see and interpret visual data"},
    {"id": "5", "title": "Data Science Best Practices", "content": "Data science combines statistics, programming, and domain expertise to extract insights from data. It involves data collection, cleaning, analysis, visualization, and interpretation to support decision-making.", "description": "Essential techniques and methodologies for effective data analysis"},
    {"id": "6", "title": "Blockchain Technology Overview", "content": "Blockchain is a distributed ledger technology that maintains a continuously growing list of records. It ensures transparency, security, and immutability in digital transactions and smart contracts.", "description": "Understanding the principles and applications of blockchain technology"},
    {"id": "7", "title": "Cloud Computing Architecture", "content": "Cloud computing delivers computing services over the internet, including servers, storage, databases, networking, and software. It offers scalability, flexibility, and cost-effectiveness for businesses.", "description": "Explore cloud platforms and deployment strategies"},
    {"id": "8", "title": "Cybersecurity Fundamentals", "content": "Cybersecurity protects digital systems, networks, and data from cyber threats. It involves implementing security measures, monitoring for vulnerabilities, and responding to security incidents.", "description": "Essential security practices and threat protection strategies"},
    {"id": "9", "title": "Mobile App Development", "content": "Mobile app development creates software applications for smartphones and tablets. It involves designing user interfaces, implementing functionality, and ensuring cross-platform compatibility.", "description": "Modern approaches to building mobile applications"},
    {"id": "10", "title": "Web Development Trends", "content": "Modern web development focuses on responsive design, progressive web apps, and performance optimization. It leverages frameworks like React, Vue, and Angular to build interactive user experiences.", "description": "Current trends and technologies in web development"},
  ])

  client.fulltext_index.enable_fulltext()
  client.execute('SET ft_relevancy_algorithm = "BM25"')
  client.fulltext_index.create("fulltext_examples", "ftidx_content", ["title", "content"], "BM25")

  # Test fulltext search - Natural Language Mode
  print("Fulltext Search Results (Natural Language Mode):")
  search_result = client.fulltext_index.simple_query("fulltext_examples").columns("title", "content").search("machine learning").execute()
  print(f"Found {len(search_result.rows)} results for 'machine learning':")
  for i, row in enumerate(search_result.rows, 1):
    try:
      score = float(row[2]) if len(row) > 2 and row[2] is not None else 0.0
      print(f"  {i}. ID: {row[0]}, Title: {row[1]}, Score: {score:.4f}")
    except (ValueError, TypeError, IndexError):
      print(f"  {i}. ID: {row[0]}, Title: {row[1]}, Score: N/A")
    print(f"     Content: {row[2][:100] if len(row) > 2 else 'N/A'}...")
    print()

  # Test fulltext search - Boolean Mode
  print("Fulltext Search Results (Boolean Mode):")
  search_result2 = client.fulltext_index.simple_query("fulltext_examples").columns("title", "content").must_have("machine", "learning").execute()
  print(f"Found {len(search_result2.rows)} results for '+machine +learning':")
  for i, row in enumerate(search_result2.rows, 1):
    print(f"  {i}. ID: {row[0]}, Title: {row[1]}")
    print(f"     Content: {row[2][:100] if len(row) > 2 else 'N/A'}...")
    print()

  # Show table structure
  result = client.execute("SHOW CREATE TABLE fulltext_examples")
  print("Table creation SQL:")
  for row in result.rows:
    print(row[1])  # The second column contains the CREATE TABLE statement




  client.drop_all(base)

  client.disconnect()
