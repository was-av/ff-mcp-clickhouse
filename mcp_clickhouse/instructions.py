import os


def load_instructions() -> str:
    """
    Load instructions from the instructions.md file.
    
    Returns:
        str: The content of the instructions file.
    """
    folder = os.path.dirname(__file__)
    with open(os.path.join(folder, "prompts/instructions.md"), "r") as f:
        return f.read()
    

mcp_clickhouse_instructions = load_instructions()