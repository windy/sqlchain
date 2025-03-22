# LLM and SQLChain Project

## Project Overview
This project demonstrates the integration of a Language Model (LLM) with SQLChain, a library for processing SQL database queries in a chainable manner. It showcases how to use LLMs for natural language to SQL conversion and how to combine this with SQLChain for efficient data processing.

## Features
- **LLM Integration**: Uses LLMs (like OpenAI) to convert natural language queries into SQL.
- **SQLChain**: Processes SQL queries using chainable operations with support for parallel processing.
- **Examples**: Includes example code demonstrating basic LLM usage, SQLChain operations, and their combined use.
- **Tests**: Comprehensive unit and integration tests to ensure code correctness.

## Installation and Setup

### Prerequisites
- Python 3.12+
- pip package installer

### Steps
1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```

2.  **Create a virtual environment:**
    ```bash
    python -m venv venv
    source venv/bin/activate   # On Windows: venv\Scripts\activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
    (Create a `requirements.txt` file with the following content):
    ```text
    openai
    httpx
    python-dotenv
    sqlalchemy
    pymysql
    beautifulsoup4
    ```

4.  **Configure environment variables:**

    *   Create a `.env` file in the project root.
    *   Add the following variables with your actual credentials:

        ```
        OPENAI_API_KEY=your_openai_api_key
        DB_HOST=localhost
        DB_PORT=3306
        DB_USERNAME=your_db_username
        DB_PASSWORD=your_db_password
        DB_DATABASE=your_db_name
        ```

## Running Examples

1.  **Navigate to the project directory:**
    ```bash
    cd <repository_directory>
    ```

2.  **Run the examples:**
    ```bash
    python examples.py <example_name>
    ```
    *   Replace `<example_name>` with one of the following:
        *   `llm_basic`: Basic LLM query.
        *   `llm_conversation`: LLM conversation example.
        *   `llm_json`: LLM JSON mode example.
        *   `sql_basic`: Basic SQLChain example.
        *   `sql_transform`: SQLChain transform example.
        *   `sql_async`: Async SQLChain example.
        *   `llm_to_sql`: LLM generating SQL for SQLChain.
        *   `nl_query`: Natural language database query example.
    *   To run all examples:
        ```bash
        python examples.py all
        ```

## Running Tests

1.  **Ensure you have the necessary testing libraries:**
    ```bash
    pip install unittest
    ```

2.  **Run the tests:**
    ```bash
    python tests.py
    ```
    *   This will execute all unit and integration tests defined in `tests.py`.

## Basic Usage Guide

### LLM Examples (from `examples.py`)

*   **Basic LLM Query:**
    ```python
    llm = LLM()
    response = llm.ask(["What is SQLAlchemy?"])
    print(response['content'])
    ```

*   **SQLChain Basic Example:**
    ```python
    engine = setup_database_engine()
    chain = SQLChain(engine)
    results = chain.sql("SELECT * FROM users LIMIT 5").collect()
    print(results)
    ```

*   **Combined LLM and SQLChain Example:**
    ```python
    llm = LLM()
    engine = setup_database_engine()
    chain = SQLChain(engine)
    schema_info = "Table: users - id (INT), username (VARCHAR), email (VARCHAR)"
    natural_query = "Find all active users"
    prompt = f"Generate SQL query for: {natural_query} based on {schema_info}"
    sql_response = llm.ask([prompt])
    results = chain.sql(sql_response['content']).collect()
    print(results)
    ```

## Notes and Limitations

*   **Security**: Exercise caution when using LLMs to generate SQL queries. Always validate and sanitize the generated SQL before execution to prevent SQL injection attacks.
*   **Database Schema**: The LLM's ability to generate accurate SQL depends on the quality and completeness of the provided database schema information.
*   **Error Handling**: Ensure robust error handling is implemented to manage potential issues with LLM API calls and database operations.

## Contributing

Contributions are welcome! Please follow these guidelines:

1.  Fork the repository.
2.  Create a new branch for your feature or bug fix.
3.  Write tests for your changes.
4.  Ensure all tests pass.
5.  Submit a pull request.