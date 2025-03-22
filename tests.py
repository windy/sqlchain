#!/usr/bin/env python3
# tests.py - Unit and integration tests for LLM and SQLChain classes

import os
import unittest
from unittest.mock import Mock, patch, MagicMock
import asyncio
import json
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Import our classes
from src.ai import LLM
from src.chain import SQLChain, Stream, ParallelStream, StreamError

# Load environment variables
load_dotenv()

class TestLLM(unittest.TestCase):
    """
    Unit tests for the LLM class.
    
    Tests initialization parameters and the ask method with various configurations.
    """
    
    def setUp(self):
        """Set up test environment before each test."""
        # Mock environment variables
        self.openai_key_patcher = patch.dict(os.environ, {"OPENAI_API_KEY": "mock-api-key"})
        self.openai_key_patcher.start()
        
        # Mock OpenAI client
        self.openai_patcher = patch('src.ai.OpenAI')
        self.mock_openai = self.openai_patcher.start()
        
        # Setup mock response
        mock_response = Mock()
        mock_choice = Mock()
        mock_message = Mock()
        mock_message.content = "This is a test response"
        mock_choice.message = mock_message
        mock_response.choices = [mock_choice]
        
        # Configure the mock OpenAI client to return our mock response
        mock_client = self.mock_openai.return_value
        mock_client.chat.completions.create.return_value = mock_response
    
    def tearDown(self):
        """Clean up after each test."""
        self.openai_key_patcher.stop()
        self.openai_patcher.stop()
    
    def test_init_with_openai(self):
        """Test initializing LLM with OpenAI API key."""
        llm = LLM()
        
        # Assert OpenAI was initialized with the right key
        self.mock_openai.assert_called_once_with(api_key='mock-api-key')
        self.assertEqual(llm.openai_key, "mock-api-key")
    
    def test_init_with_ollama(self):
        """Test initializing LLM with Ollama URL."""
        ollama_url = "http://localhost:11434/v1"
        llm = LLM(ollama_url=ollama_url)
        
        # Assert OpenAI was initialized with Ollama configuration
        self.mock_openai.assert_called_once_with(api_key='ollama', base_url=ollama_url)
        self.assertEqual(llm.openai_key, "mock-api-key")  # Should still load the key
        
    def test_ask_with_string(self):
        """Test ask method with string input."""
        llm = LLM()
        query = "What is SQLAlchemy?"
        response = llm.ask([query])
        
        # Assert the client was called with the correct parameters
        mock_client = self.mock_openai.return_value
        mock_client.chat.completions.create.assert_called_once()
        call_args = mock_client.chat.completions.create.call_args[1]
        
        # Check the messages structure
        self.assertEqual(len(call_args['messages']), 2)  # System message + user message
        self.assertEqual(call_args['messages'][0]['role'], 'system')
        self.assertEqual(call_args['messages'][1]['role'], 'user')
        self.assertEqual(call_args['messages'][1]['content'], query)
        
        # Check the response format
        self.assertEqual(response['role'], 'assistant')
        self.assertEqual(response['content'], "This is a test response")
    
    def test_ask_with_conversation(self):
        """Test ask method with conversation history."""
        llm = LLM()
        
        # Create a conversation history
        conversation = [
            {"role": "user", "content": "What is Python?"},
            {"role": "assistant", "content": "Python is a programming language."},
            {"role": "user", "content": "How is it used?"}
        ]
        
        response = llm.ask(conversation)
        
        # Assert the client was called with the correct conversation history
        mock_client = self.mock_openai.return_value
        call_args = mock_client.chat.completions.create.call_args[1]
        
        # Check the messages structure includes all conversation entries
        self.assertEqual(len(call_args['messages']), 4)  # System message + 3 conversation messages
        self.assertEqual(call_args['messages'][1]['role'], 'user')
        self.assertEqual(call_args['messages'][1]['content'], "What is Python?")
        self.assertEqual(call_args['messages'][2]['role'], 'assistant')
        self.assertEqual(call_args['messages'][3]['role'], 'user')
        self.assertEqual(call_args['messages'][3]['content'], "How is it used?")
    
    def test_ask_with_json_mode(self):
        """Test ask method with JSON mode enabled."""
        llm = LLM()
        query = "List Python web frameworks"
        response = llm.ask([query], json_mode=True)
        
        # Assert JSON response format was requested
        mock_client = self.mock_openai.return_value
        call_args = mock_client.chat.completions.create.call_args[1]
        
        self.assertEqual(call_args['response_format'], {"type": "json_object"})
    
    def test_ask_with_custom_model(self):
        """Test ask method with custom model specified."""
        llm = LLM()
        query = "What is SQLAlchemy?"
        custom_model = "gpt-4"
        response = llm.ask([query], model=custom_model)
        
        # Assert the correct model was used
        mock_client = self.mock_openai.return_value
        call_args = mock_client.chat.completions.create.call_args[1]
        
        self.assertEqual(call_args['model'], custom_model)


class TestSQLChain(unittest.TestCase):
    """
    Unit tests for the SQLChain class.
    
    Tests SQL execution and stream processing functionality.
    """
    
    def setUp(self):
        """Set up test environment before each test."""
        # Mock SQLAlchemy engine
        self.engine_patcher = patch('sqlalchemy.create_engine')
        self.mock_create_engine = self.engine_patcher.start()
        
        # Configure mock engine
        self.mock_engine = Mock()
        self.mock_create_engine.return_value = self.mock_engine
        
        # Mock connection and execution result
        self.mock_connection = Mock()
        self.mock_engine.connect.return_value.__enter__.return_value = self.mock_connection
        
        # Setup mock query result
        mock_result = Mock()
        mock_mappings = Mock()
        mock_mappings.all.return_value = [
            {"id": 1, "name": "Test User 1", "email": "user1@example.com"},
            {"id": 2, "name": "Test User 2", "email": "user2@example.com"}
        ]
        mock_result.mappings.return_value = mock_mappings
        self.mock_connection.execute.return_value = mock_result
        
        # Create SQLChain instance
        self.chain = SQLChain(self.mock_engine)
    
    def tearDown(self):
        """Clean up after each test."""
        self.engine_patcher.stop()
    
    def test_sql_execution(self):
        """Test basic SQL query execution."""
        # Execute a simple query
        query = "SELECT * FROM users"
        results = self.chain.sql(query).collect()
        
        # Verify the connection and execute were called
        self.mock_engine.connect.assert_called_once()
        self.mock_connection.execute.assert_called_once()
        
        # Check that text() was used with our query
        call_args = self.mock_connection.execute.call_args
        self.assertEqual(str(call_args[0][0]), query)
        
        # Check results
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['id'], 1)
        self.assertEqual(results[0]['name'], "Test User 1")
        self.assertEqual(results[1]['id'], 2)
    
    def test_sql_with_parameters(self):
        """Test SQL execution with parameters."""
        # Execute a query with parameters
        query = "SELECT * FROM users WHERE id = :user_id"
        params = {"user_id": 1}
        self.chain.sql(query, params).collect()
        
        # Verify execute was called with parameters
        self.mock_connection.execute.assert_called_once()
        call_args = self.mock_connection.execute.call_args
        self.assertEqual(str(call_args[0][0]), query)
        self.assertEqual(call_args[0][1], params)
    
    def test_map_operation(self):
        """Test map operation on query results."""
        # Execute query with map transformation
        results = (self.chain.sql("SELECT * FROM users")
                  .map(lambda user: {
                      "id": user["id"],
                      "full_name": user["name"],
                      "contact": user["email"]
                  })
                  .collect())
        
        # Check transformed results
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['id'], 1)
        self.assertEqual(results[0]['full_name'], "Test User 1")
        self.assertEqual(results[0]['contact'], "user1@example.com")
        
        # Original fields should not be present
        self.assertNotIn('name', results[0])
        self.assertNotIn('email', results[0])
    
    def test_filter_operation(self):
        """Test filter operation on query results."""
        # Execute query with filter transformation
        results = (self.chain.sql("SELECT * FROM users")
                  .filter(lambda user: user["id"] == 1)
                  .collect())
        
        # Check filtered results
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['id'], 1)
        self.assertEqual(results[0]['name'], "Test User 1")
    
    def test_chain_operations(self):
        """Test chaining multiple operations together."""
        # Execute query with map and filter
        results = (self.chain.sql("SELECT * FROM users")
                  .map(lambda user: {
                      "id": user["id"],
                      "full_name": user["name"],
                      "contact": user["email"]
                  })
                  .filter(lambda user: user["id"] == 2)
                  .collect())
        
        # Check results after both operations
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['id'], 2)
        self.assertEqual(results[0]['full_name'], "Test User 2")
        self.assertEqual(results[0]['contact'], "user2@example.com")
    
    def test_error_handling(self):
        """Test error handling during SQL execution."""
        # Mock the connection to raise an exception
        self.mock_engine.connect.return_value.__enter__.side_effect = Exception("Database connection error")
        
        # Execute query and expect an exception
        with self.assertRaises(StreamError):
            self.chain.sql("SELECT * FROM users").collect()


class TestIntegration(unittest.TestCase):
    """
    Integration tests for LLM and SQLChain classes working together.
    
    Tests the complete flow from natural language to SQL execution.
    """
    
    def setUp(self):
        """Set up test environment before each test."""
        # Mock environment variables
        self.env_patcher = patch.dict(os.environ, {
            "OPENAI_API_KEY": "mock-api-key",
            "DB_USERNAME": "test_user",
            "DB_PASSWORD": "test_pass",
            "DB_HOST": "localhost",
            "DB_PORT": "3306",
            "DB_DATABASE": "test_db"
        })
        self.env_patcher.start()
        
        # Mock OpenAI client
        self.openai_patcher = patch('src.ai.OpenAI')
        self.mock_openai = self.openai_patcher.start()
        
        # Setup mock LLM response for SQL generation
        mock_response = Mock()
        mock_choice = Mock()
        mock_message = Mock()
        mock_message.content = "SELECT * FROM users WHERE status = 'active' AND created_at > DATE_SUB(NOW(), INTERVAL 30 DAY)"
        mock_choice.message = mock_message
        mock_response.choices = [mock_choice]
        
        # Configure the mock OpenAI client
        mock_client = self.mock_openai.return_value
        mock_client.chat.completions.create.return_value = mock_response
        
        # Mock SQLAlchemy engine
        self.engine_patcher = patch('sqlalchemy.create_engine')
        self.mock_create_engine = self.engine_patcher.start()
        
        # Configure mock database
        self.mock_engine = Mock()
        self.mock_create_engine.return_value = self.mock_engine
        
        # Mock connection and execution result
        self.mock_connection = Mock()
        self.mock_engine.connect.return_value.__enter__.return_value = self.mock_connection
        
        # Setup mock query result
        mock_result = Mock()
        mock_mappings = Mock()
        mock_mappings.all.return_value = [
            {"id": 1, "username": "new_user1", "email": "user1@example.com", "created_at": "2023-11-01", "status": "active"},
            {"id": 2, "username": "new_user2", "email": "user2@example.com", "created_at": "2023-11-02", "status": "active"}
        ]
        mock_result.mappings.return_value = mock_mappings
        self.mock_connection.execute.return_value = mock_result
        
        # Create instances
        self.llm = LLM()
        self.chain = SQLChain(self.mock_engine)
    
    def tearDown(self):
        """Clean up after each test."""
        self.env_patcher.stop()
        self.openai_patcher.stop()
        self.engine_patcher.stop()
    
    def test_natural_language_to_sql(self):
        """Test converting natural language query to SQL and executing it."""
        # Natural language query
        natural_query = "Find all active users who joined in the last 30 days"
        
        # Schema information for context
        schema_info = """
        Table: users
        - id (INT): Primary key
        - username (VARCHAR): User's login name
        - email (VARCHAR): User's email address
        - created_at (DATETIME): When the user was created
        - status (VARCHAR): User's status (active, inactive, suspended)
        """
        
        # Ask LLM to generate SQL
        prompt = f"""
        Based on the following database schema:
        
        {schema_info}
        
        Generate a SQL query to: {natural_query}
        
        Return only the SQL query itself without any explanations or markdown formatting.
        """
        
        sql_response = self.llm.ask([prompt])
        generated_sql = sql_response['content']
        
        # Execute the generated SQL
        results = self.chain.sql(generated_sql).collect()
        
        # Check the SQL was generated and executed correctly
        self.mock_openai.return_value.chat.completions.create.assert_called_once()
        self.mock_connection.execute.assert_called_once()
        
        # Verify results
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['username'], 'new_user1')
        self.assertEqual(results[1]['username'], 'new_user2')
    
    def test_query_and_summarize_results(self):
        """Test querying database and summarizing results with LLM."""
        # First LLM call returns SQL
        first_response = Mock()
        first_choice = Mock()
        first_message = Mock()
        first_message.content = "SELECT * FROM products WHERE category = 'electronics' AND price < 1000 ORDER BY rating DESC LIMIT 5"
        first_choice.message = first_message
        first_response.choices = [first_choice]
        
        # Second LLM call returns summary
        second_response = Mock()
        second_choice = Mock()
        second_message = Mock()
        second_message.content = "The top 5 highest-rated electronics products under $1000 are the Ultra HD Monitor, Wireless Headphones Pro, Smart Home Hub, Gaming Laptop, and Noise-Cancelling Earbuds."
        second_choice.message = second_message
        second_response.choices = [second_choice]
        
        # Configure mock to return different responses on consecutive calls
        self.mock_openai.return_value.chat.completions.create.side_effect = [first_response, second_response]
        
        # Mock database results for this specific query
        mock_result = Mock()
        mock_mappings = Mock()
        mock_mappings.all.return_value = [
            {'id': 101, 'name': 'Ultra HD Monitor', 'category': 'electronics', 'price': '899.99', 'stock': 45, 'rating': 4.9},
            {'id': 102, 'name': 'Wireless Headphones Pro', 'category': 'electronics', 'price': '349.99', 'stock': 120, 'rating': 4.8},
            {'id': 103, 'name': 'Smart Home Hub', 'category': 'electronics', 'price': '199.99', 'stock': 78, 'rating': 4.7},
            {'id': 104, 'name': 'Gaming Laptop', 'category': 'electronics', 'price': '999.99', 'stock': 15, 'rating': 4.7},
            {'id': 105, 'name': 'Noise-Cancelling Earbuds', 'category': 'electronics', 'price': '249.99', 'stock': 62, 'rating': 4.6}
        ]
        mock_result.mappings.return_value = mock_mappings
        self.mock_connection.execute.return_value = mock_result
        
        # Schema information
        schema_info = """
        Table: products
        - id (INT): Primary key
        - name (VARCHAR): Product name
        - category (VARCHAR): Product category
        - price (DECIMAL): Product price
        - stock (INT): Current stock level
        - rating (FLOAT): Average customer rating (1-5)
        """
        
        # Natural language query
        user_query = "What are the top 5 highest-rated products in the electronics category that cost less than $1000?"
        
        # Step 1: Generate SQL
        sql_prompt = f"""
        Based on the following database schema:
        
        {schema_info}
        
        Generate a SQL query to answer this question: {user_query}
        
        Return only the SQL query without any explanations or markdown formatting.
        """
        
        sql_response = self.llm.ask([sql_prompt])
        generated_sql = sql_response['content']
        
        # Step 2: Execute SQL
        results = self.chain.sql(generated_sql).collect()
        
        # Step 3: Summarize results
        summary_prompt = f"""
        Based on the query: "{user_query}"
        
        The database returned the following results:
        {json.dumps(results, indent=2)}
        
        Please provide a clear, concise summary of these results in natural language.
        """
        
        summary_response = self.llm.ask([summary_prompt])
        results_summary = summary_response['content']
        
        # Verify that both LLM calls were made
        self.assertEqual(self.mock_openai.return_value.chat.completions.create.call_count, 2)
        
        # Verify the SQL was executed
        self.mock_connection.execute.assert_called_once()
        
        # Verify the expected summary was produced
        self.assertIn("Ultra HD Monitor", results_summary)


class TestAsyncSQLChain(unittest.IsolatedAsyncioTestCase):
    """
    Tests for asynchronous operations in SQLChain.
    
    Tests parallel processing capabilities with async/await.
    """
    
    async def asyncSetUp(self):
        """Set up test environment before each test."""
        # Mock SQLAlchemy engine
        self.engine_patcher = patch('sqlalchemy.create_engine')
        self.mock_create_engine = self.engine_patcher.start()
        
        # Configure mock engine
        self.mock_engine = Mock()
        self.mock_create_engine.return_value = self.mock_engine
        
        # Mock connection and execution result
        self.mock_connection = Mock()
        self.mock_engine.connect.return_value.__enter__.return_value = self.mock_connection
        
        # Setup mock query result
        mock_result = Mock()
        mock_mappings = Mock()
        mock_mappings.all.return_value = [
            {"id": 1, "name": "Product 1", "price": "100.00"},
            {"id": 2, "name": "Product 2", "price": "200.00"},
            {"id": 3, "name": "Product 3", "price": "300.00"},
            {"id": 4, "name": "Product 4", "price": "400.00"},
            {"id": 5, "name": "Product 5", "price": "500.00"}
        ]
        mock_result.mappings.return_value = mock_mappings
        self.mock_connection.execute.return_value = mock_result
        
        # Create SQLChain instance
        self.chain = SQLChain(self.mock_engine)
    
    async def asyncTearDown(self):
        """Clean up after each test."""
        self.engine_patcher.stop()
    
    async def test_parallel_map(self):
        """Test parallel map operation."""
        # Use parallel map to process results
        results = await (self.chain.sql("SELECT * FROM products")
                        .parallel(num_workers=2, chunk_size=2)
                        .map(lambda product: {
                            "id": product["id"],
                            "name": product["name"],
                            "price_in_euros": float(product["price"]) * 0.85
                        })
                        .collect())
        
        # Verify results were transformed correctly
        self.assertEqual(len(results), 5)
        self.assertEqual(results[0]["id"], 1)
        self.assertAlmostEqual(results[0]["price_in_euros"], 85.0)
        self.assertEqual(results[4]["id"], 5)
        self.assertAlmostEqual(results[4]["price_in_euros"], 425.0)
    
    async def test_parallel_filter(self):
        """Test parallel filter operation."""
        # Use parallel filter to select specific results
        results = await (self.chain.sql("SELECT * FROM products")
                        .parallel(num_workers=2, chunk_size=2)
                        .filter(lambda product: float(product["price"]) > 300)
                        .collect())
        
        # Verify filtering worked correctly
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["id"], 4)
        self.assertEqual(results[1]["id"], 5)
    
    async def test_parallel_reduce(self):
        """Test parallel reduce operation."""
        # Use parallel reduce to calculate total price
        total = await (self.chain.sql("SELECT * FROM products")
                      .parallel(num_workers=2, chunk_size=2)
                      .map(lambda product: float(product["price"]))
                      .reduce(lambda x, y: x + y, initial=0))
        
        # Verify reduction worked correctly
        self.assertEqual(total, 1500.0)

    async def test_parallel_chaining(self):
        """Test chaining multiple parallel operations."""
        # Chain parallel operations: map -> filter -> map
        results = await (self.chain.sql("SELECT * FROM products")
                        .parallel(num_workers=2, chunk_size=2)
                        .map(lambda product: {
                            "id": product["id"],
                            "name": product["name"],
                            "price_with_tax": float(product["price"]) * 1.2
                        })
                        .filter(lambda product: product["price_with_tax"] > 300)
                        .map(lambda product: {
                            "id": product["id"],
                            "name": product["name"],
                            "final_price": round(product["price_with_tax"], 2)
                        })
                        .collect())
        
        # Verify the chain of operations worked correctly
        self.assertEqual(len(results), 3)  # Products with prices over 250 after tax
        self.assertEqual(results[0]["id"], 3)  # 300 * 1.2 = 360
        self.assertEqual(results[1]["id"], 4)  # 400 * 1.2 = 480
        self.assertEqual(results[2]["id"], 5)  # 500 * 1.2 = 600
        self.assertEqual(results[0]["final_price"], 360.0)


if __name__ == '__main__':
    unittest.main()