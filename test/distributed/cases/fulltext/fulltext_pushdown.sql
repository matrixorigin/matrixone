-- fulltext pushdown: verify correctness of fulltext search with additional filter predicates
-- The BloomFilter pushdown is transparent to query results, so we verify result correctness.

drop database if exists ft_pushdown_test;
create database ft_pushdown_test;
use ft_pushdown_test;

create table articles (
    id bigint primary key,
    title varchar(200) not null,
    body text not null,
    category varchar(50) not null,
    status int not null default 1
);

insert into articles values
(1,  'Introduction to Machine Learning', 'Machine learning is a subset of artificial intelligence that focuses on building systems that learn from data', 'tech', 1),
(2,  'Deep Learning Fundamentals', 'Deep learning uses neural networks with many layers to learn representations of data', 'tech', 1),
(3,  'Natural Language Processing', 'NLP combines computational linguistics with machine learning to process human language', 'tech', 1),
(4,  'Database Systems Overview', 'Modern database systems support both transactional and analytical workloads efficiently', 'tech', 1),
(5,  'Cloud Computing Basics', 'Cloud computing delivers computing services over the internet including servers storage and databases', 'tech', 0),
(6,  'Cooking with Machine Learning', 'Using machine learning algorithms to recommend recipes based on available ingredients', 'food', 1),
(7,  'The Art of French Cooking', 'French cooking techniques have influenced cuisines around the world for centuries', 'food', 1),
(8,  'Italian Pasta Guide', 'A comprehensive guide to making authentic Italian pasta from scratch', 'food', 1),
(9,  'Healthy Eating Habits', 'Building healthy eating habits requires understanding nutrition and meal planning', 'food', 0),
(10, 'Sports Analytics with ML', 'Machine learning is transforming sports analytics and player performance prediction', 'sports', 1),
(11, 'Basketball Strategy Guide', 'Modern basketball strategy relies heavily on data analytics and machine learning models', 'sports', 1),
(12, 'Running for Beginners', 'A beginners guide to running including training plans and injury prevention tips', 'sports', 1),
(13, 'Swimming Techniques', 'Advanced swimming techniques for competitive swimmers and triathletes', 'sports', 0),
(14, 'Machine Learning in Finance', 'Financial institutions use machine learning for fraud detection and risk assessment', 'finance', 1),
(15, 'Investment Strategies', 'Long term investment strategies for building wealth through diversified portfolios', 'finance', 1),
(16, 'Cryptocurrency Overview', 'Understanding blockchain technology and cryptocurrency markets', 'finance', 0),
(17, 'Travel Guide to Japan', 'Exploring Japanese culture food and technology during your visit to Japan', 'travel', 1),
(18, 'European Road Trip', 'Planning the perfect European road trip through France Italy and Spain', 'travel', 1),
(19, 'Machine Learning Research Papers', 'A curated list of the most influential machine learning research papers of the decade', 'tech', 1),
(20, 'Data Engineering Best Practices', 'Best practices for building robust data pipelines and data infrastructure', 'tech', 1),
(21, 'Reinforcement Learning Applications', 'Reinforcement learning applications in robotics gaming and autonomous systems', 'tech', 1),
(22, 'Computer Vision Advances', 'Recent advances in computer vision powered by deep learning and neural networks', 'tech', 1),
(23, 'Graph Neural Networks', 'Graph neural networks for learning on graph structured data and social networks', 'tech', 1),
(24, 'AutoML and Neural Architecture Search', 'Automated machine learning and neural architecture search for model optimization', 'tech', 1),
(25, 'Federated Learning', 'Federated learning enables training machine learning models across decentralized data', 'tech', 1),
(26, 'MLOps and Model Deployment', 'Best practices for deploying machine learning models in production environments', 'tech', 1),
(27, 'Transfer Learning Guide', 'Transfer learning allows reusing pre-trained machine learning models for new tasks', 'tech', 1),
(28, 'Explainable AI', 'Making machine learning models interpretable and explainable for critical applications', 'tech', 1),
(29, 'Edge Computing and ML', 'Running machine learning inference on edge devices for real time applications', 'tech', 1),
(30, 'Quantum Machine Learning', 'Exploring the intersection of quantum computing and machine learning algorithms', 'tech', 0);

create fulltext index ftidx_articles on articles (title, body);

-- scenario 1: pure fulltext search (no extra filter)
select id, title, category from articles
where match(title, body) against('machine learning')
order by id;

-- scenario 2: fulltext search + category filter (triggers pushdown)
select id, title, category from articles
where match(title, body) against('machine learning')
  and category = 'tech'
order by id;

-- scenario 3: fulltext search + multiple filters
select id, title, category, status from articles
where match(title, body) against('machine learning')
  and category = 'tech'
  and status = 1
order by id;

-- scenario 4: fulltext search + range filter
select id, title, category from articles
where match(title, body) against('machine learning')
  and id > 10
order by id;

-- scenario 5: fulltext search + filter + count
select count(*) from articles
where match(title, body) against('machine learning')
  and category = 'tech';

drop database ft_pushdown_test;
