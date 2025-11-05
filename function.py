import ast

def extract_classes_and_functions(filepath, output_txt):
    with open(filepath, "r", encoding="utf-8") as f:
        source = f.read()
    
    # Parse the Python source into an AST
    tree = ast.parse(source)
    
    results = []
    
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            results.append(f"Class: {node.name}")
        
        elif isinstance(node, ast.FunctionDef):
            results.append(f"Function: {node.name}")
            
            # Extract return statements inside this function
            for child in ast.walk(node):
                if isinstance(child, ast.Return):
                    if child.value is not None:
                        try:
                            return_val = ast.unparse(child.value)  # Python 3.9+
                        except AttributeError:
                            # fallback for older python
                            return_val = ast.dump(child.value)
                    else:
                        return_val = "None"
                    results.append(f"  Return: {return_val}")
    
    # Write results to txt file
    with open(output_txt, "w", encoding="utf-8") as f:
        f.write("scrape_pipeline/config/scraping_config.py\n")
        for line in results:
            f.write(line + "\n")


# Example usage
extract_classes_and_functions("tes.py", "output.txt")