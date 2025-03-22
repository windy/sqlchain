#!/usr/bin/env python3
# examples.py - Demonstrates usage of LLM and SQLChain classes

import os
import asyncio
from typing import Dict, Any, List, Optional
import argparse
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Import our classes
from src.ai import LLM
from src.chain import SQLChain, Stream, ParallelStream

# Load environment variables
load_dotenv()

def setup_database_engine():
    """
    Set up and return a SQLAlchemy database engine using environment variables.
    
    Returns:
        SQLAlchemy Engine: Configured database engine
    """
    db_username = os.getenv('DB_USERNAME')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '3306')
    db_database = os.getenv('DB_DATABASE')
    
    # Create connection URL
    connection_url = f"mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_database}"
    
    # Create and return the engine
    return create_engine(connection_url, pool_size=5, max_overflow=5)

# ============ LLM Examples ============

def llm_basic_example():
    """
    Basic example of using the LLM class for a simple query.
    
    Demonstrates how to initialize the LLM and make a simple request.
    """
    print("\n=== Basic LLM Example ===")
    
    # Initialize LLM - will use OpenAI by default
    llm = LLM()
    
    # Create a simple query
    query = "What is SQLAlchemy and how is it used in Python?"
    
    # Send the request and get response
    print(f"Sending query: {query}")
    response = llm.ask([query])
    
    print(f"Response from LLM:\n{response['content']}\n")
    return response

def llm_conversation_example():
    """
    Example showing a multi-turn conversation with the LLM.
    
    Demonstrates how chat history is managed with the LLM class.
    """
    print("\n=== LLM Conversation Example ===")
    
    # Initialize LLM
    llm = LLM()
    
    # Create a conversation history
    chat_history = []
    
    # First user message
    user_message_1 = "What are the key features of Python?"
    print(f"User: {user_message_1}")
    chat_history.append({"role": "user", "content": user_message_1})
    
    # Get first response
    response_1 = llm.ask(chat_history)
    print(f"Assistant: {response_1['content']}")
    chat_history.append(response_1)
    
    # Second user message, building on the conversation
    user_message_2 = "How does Python compare to JavaScript for web development?"
    print(f"User: {user_message_2}")
    chat_history.append({"role": "user", "content": user_message_2})
    
    # Get second response
    response_2 = llm.ask(chat_history)
    print(f"Assistant: {response_2['content']}")
    
    return chat_history

def llm_json_mode_example():
    """
    Example showing how to use the LLM with JSON mode.
    
    Demonstrates receiving structured data from the LLM.
    """
    print("\n=== LLM JSON Mode Example ===")
    
    # Initialize LLM
    llm = LLM()
    
    # Create a query that should return structured data
    query = "List the top 3 Python web frameworks with their key features. Format as JSON with 'frameworks' as the key."
    
    # Send request with JSON mode enabled
    print(f"Sending query: {query}")
    response = llm.ask([query], json_mode=True)
    
    # Parse and display the JSON response
    try:
        json_data = json.loads(response['content'])
        print("Received structured JSON data:")
        print(json.dumps(json_data, indent=2))
    except json.JSONDecodeError:
        print("Warning: Response could not be parsed as JSON")
        print(f"Raw response: {response['content']}")
    
    return response

# ============ SQLChain Examples ============

def sqlchain_basic_example():
    """
    Basic example of using SQLChain to execute a query and process results.
    
    Demonstrates database connection and simple query execution.
    """
    print("\n=== Basic SQLChain Example ===")
    
    try:
        # Create database engine
        engine = setup_database_engine()
        
        # Initialize SQLChain
        chain = SQLChain(engine)
        
        # Execute a simple query - adjust table name based on your database
        query = "SELECT * FROM users LIMIT 5"
        print(f"Executing query: {query}")
        
        # Execute query and collect results
        results = chain.sql(query).collect()
        
        # Display results
        print(f"Query returned {len(results)} rows:")
        for row in results:
            print(row)
        
        # Clean up
        engine.dispose()
        return results
        
    except Exception as e:
        print(f"Error in SQLChain example: {str(e)}")
        return None

def sqlchain_transform_example():
    """
    Example showing how to transform data using SQLChain operations.
    
    Demonstrates map and filter operations on query results.
    """
    print("\n=== SQLChain Transform Example ===")
    
    try:
        # Create database engine
        engine = setup_database_engine()
        
        # Initialize SQLChain
        chain = SQLChain(engine)
        
        # Execute query - adjust table name based on your database
        query = "SELECT * FROM products LIMIT 10"
        print(f"Executing query: {query}")
        
        # Execute query with transformations
        results = (chain.sql(query)
                  .map(lambda row: {
                      'id': row['id'],
                      'name': row['name'],
                      'price_with_tax': float(row['price']) * 1.1
                  })
                  .filter(lambda row: float(row['price_with_tax']) > 50)
                  .collect())
        
        # Display results
        print(f"Query returned {len(results)} filtered rows:")
        for row in results:
            print(f"Product {row['name']} (ID: {row['id']}): ${row['price_with_tax']:.2f} with tax")
        
        # Clean up
        engine.dispose()
        return results
        
    except Exception as e:
        print(f"Error in SQLChain transform example: {str(e)}")
        return None

# ============ Async SQLChain Examples ============

async def sqlchain_async_example():
    """
    Example showing asynchronous execution with SQLChain.
    
    Demonstrates parallel processing of query results.
    """
    print("\n=== SQLChain Async Example ===")
    
    try:
        # Create database engine
        engine = setup_database_engine()
        
        # Initialize SQLChain
        chain = SQLChain(engine)
        
        # Execute query with parallel processing - adjust table name based on your database
        query = "SELECT * FROM orders LIMIT 100"
        print(f"Executing async query: {query}")
        
        # Define a computation-heavy function to demonstrate parallel processing
        async def process_order(order):
            # Simulate processing time
            await asyncio.sleep(0.1)
            return {
                'order_id': order['id'],
                'total': float(order['total']),
                'processed': True
            }
        
        # Execute query with parallel transformations
        results = await (chain.sql(query)
                        .parallel(num_workers=4, chunk_size=10)
                        .map(lambda order: asyncio.run(process_order(order)))
                        .collect())
        
        # Display results
        print(f"Async query processed {len(results)} orders:")
        for i, result in enumerate(results[:5]):  # Show first 5 only
            print(f"Order {result['order_id']}: ${result['total']:.2f}")
        
        if len(results) > 5:
            print(f"...and {len(results) - 5} more")
        
        # Clean up
        engine.dispose()
        return results
        
    except Exception as e:
        print(f"Error in SQLChain async example: {str(e)}")
        return None

# ============ Combined LLM and SQLChain Examples ============

def llm_to_sql_example():
    """
    Example showing how to use LLM to generate SQL queries for SQLChain.
    
    Demonstrates using LLM output as SQL input for database queries.
    """
    print("\n=== LLM to SQL Example ===")
    
    try:
        # Initialize LLM
        llm = LLM()
        
        # Create database engine
        engine = setup_database_engine()
        
        # Initialize SQLChain
        chain = SQLChain(engine)
        
        # First, get table schema to help LLM generate accurate SQL
        # This would be a real query in a real application
        schema_query = """
        SELECT 
            TABLE_NAME, 
            COLUMN_NAME, 
            DATA_TYPE 
        FROM 
            INFORMATION_SCHEMA.COLUMNS 
        WHERE 
            TABLE_SCHEMA = 'your_database_name'
        ORDER BY 
            TABLE_NAME, ORDINAL_POSITION;
        """
        
        # For demo purposes, we'll assume a standard users table schema
        schema_info = """
        Table: users
        - id (INT): Primary key
        - username (VARCHAR): User's login name
        - email (VARCHAR): User's email address
        - created_at (DATETIME): When the user was created
        - status (VARCHAR): User's status (active, inactive, suspended)
        """
        
        # Ask LLM to generate a SQL query
        natural_query = "Find all active users who joined in the last 30 days"
        prompt = f"""
        Based on the following database schema:
        
        {schema_info}
        
        Generate a SQL query to: {natural_query}
        
        Return only the SQL query itself without any explanations or markdown formatting.
        """
        
        print(f"Asking LLM to generate SQL for: {natural_query}")
        sql_response = llm.ask([prompt])
        generated_sql = sql_response['content'].strip()
        
        # Clean up the SQL (in case LLM adds markdown or extra text)
        if "```sql" in generated_sql:
            # Extract SQL from potential code blocks
            generated_sql = generated_sql.split("```sql")[1].split("```")[0].strip()
        
        print(f"Generated SQL: {generated_sql}")
        
        # Now execute the generated SQL using SQLChain
        # In a real app, you'd want to validate/sanitize this SQL first!
        # This is just for demonstration purposes
        print("Executing generated SQL...")
        
        # In a real app, we'd execute this:
        # results = chain.sql(generated_sql).collect()
        
        # For demonstration without a real database:
        print("Query would return recent active users")
        
        # Simulate results for demonstration
        mock_results = [
            {'id': 1, 'username': 'newuser1', 'email': 'newuser1@example.com', 'created_at': '2023-11-01', 'status': 'active'},
            {'id': 2, 'username': 'newuser2', 'email': 'newuser2@example.com', 'created_at': '2023-11-02', 'status': 'active'},
        ]
        
        print(f"Mock results: {mock_results}")
        
        # Clean up
        engine.dispose()
        return {
            'natural_query': natural_query,
            'generated_sql': generated_sql,
            'results': mock_results
        }
        
    except Exception as e:
        print(f"Error in LLM to SQL example: {str(e)}")
        return None

async def natural_language_database_query():
    """
    Advanced example combining LLM and SQLChain for natural language database queries.
    
    Demonstrates full pipeline from natural language to SQL to results and back to natural language summary.
    """
    print("\n=== Natural Language Database Query Example ===")
    
    try:
        # Initialize LLM
        llm = LLM()
        
        # Create database engine
        engine = setup_database_engine()
        
        # Initialize SQLChain
        chain = SQLChain(engine)
        
        # Schema information (would be from database in real app)
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
        
        print(f"Processing natural language query: {user_query}")
        
        # Step 1: Generate SQL from natural language
        sql_prompt = f"""
        Based on the following database schema:
        
        {schema_info}
        
        Generate a SQL query to answer this question: {user_query}
        
        Return only the SQL query without any explanations or markdown formatting.
        """
        
        sql_response = llm.ask([sql_prompt])
        generated_sql = sql_response['content'].strip()
        
        # Clean up the SQL (in case LLM adds markdown or extra text)
        if "```sql" in generated_sql:
            generated_sql = generated_sql.split("```sql")[1].split("```")[0].strip()
        
        print(f"Generated SQL: {generated_sql}")
        
        # Step 2: Execute the SQL (in real app)
        # results = chain.sql(generated_sql).collect()
        
        # For demonstration without a real database:
        mock_results = [
            {'id': 101, 'name': 'Ultra HD Monitor', 'category': 'electronics', 'price': '899.99', 'stock': 45, 'rating': 4.9},
            {'id': 102, 'name': 'Wireless Headphones Pro', 'category': 'electronics', 'price': '349.99', 'stock': 120, 'rating': 4.8},
            {'id': 103, 'name': 'Smart Home Hub', 'category': 'electronics', 'price': '199.99', 'stock': 78, 'rating': 4.7},
            {'id': 104, 'name': 'Gaming Laptop', 'category': 'electronics', 'price': '999.99', 'stock': 15, 'rating': 4.7},
            {'id': 105, 'name': 'Noise-Cancelling Earbuds', 'category': 'electronics', 'price': '249.99', 'stock': 62, 'rating': 4.6},
        ]
        
        # Step 3: Generate natural language summary of results
        summary_prompt = f"""
        Based on the query: "{user_query}"
        
        The database returned the following results:
        {json.dumps(mock_results, indent=2)}
        
        Please provide a clear, concise summary of these results in natural language.
        """
        
        summary_response = llm.ask([summary_prompt])
        results_summary = summary_response['content']
        
        print("\nResults Summary:")
        print(results_summary)
        
        # Clean up
        engine.dispose()
        
        return {
            'user_query': user_query,
            'generated_sql': generated_sql,
            'results': mock_results,
            'summary': results_summary
        }
        
    except Exception as e:
        print(f"Error in natural language database query: {str(e)}")
        return None

# ============ Main Function ============

async def main():
    """
    Main function that parses command line arguments and runs the appropriate example.
    """
    parser = argparse.ArgumentParser(description='Run examples for LLM and SQLChain classes')
    parser.add_argument('example', type=str, nargs='?', default='all',
                      help='Which example to run: llm_basic, llm_conversation, llm_json, sql_basic, sql_transform, '
                           'sql_async, llm_to_sql, nl_query, or all')
    
    args = parser.parse_args()
    
    # Map of example names to functions
    examples = {
        'llm_basic': llm_basic_example,
        'llm_conversation': llm_conversation_example,
        'llm_json': llm_json_mode_example,
        'sql_basic': sqlchain_basic_example,
        'sql_transform': sqlchain_transform_example,
        'sql_async': sqlchain_async_example,
        'llm_to_sql': llm_to_sql_example,
        'nl_query': natural_language_database_query,
    }
    
    if args.example == 'all':
        # Run all examples
        print("Running all examples sequentially\n")
        
        # Run synchronous examples
        for name in ['llm_basic', 'llm_conversation', 'llm_json', 'sql_basic', 'sql_transform', 'llm_to_sql']:
            await asyncio.sleep(0.1)  # Small delay between examples for readability
            examples[name]()
        
        # Run asynchronous examples
        await examples['sql_async']()
        await examples['nl_query']()
        
    elif args.example in examples:
        # Run the specified example
        print(f"Running example: {args.example}\n")
        
        if args.example in ['sql_async', 'nl_query']:
            # Run async examples
            await examples[args.example]()
        else:
            # Run sync examples
            examples[args.example]()
    else:
        # Invalid example name
        print(f"Error: Unknown example '{args.example}'. Choose from: {', '.join(examples.keys())}, or 'all'")

if __name__ == "__main__":
    asyncio.run(main())