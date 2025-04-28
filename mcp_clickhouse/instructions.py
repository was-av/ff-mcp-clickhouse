import os


def load_instructions() -> str:
    """
    Load instructions from the instructions.md file.
    
    Returns:
        str: The content of the instructions file.
    """
    folder = os.path.dirname(__file__)
    filename = os.path.join(folder, "prompts/instructions.md")
    with open(filename, "r", encoding="utf-8") as f:
        return f.read()
    

mcp_clickhouse_instructions = load_instructions()
