node_properties_query = """
MATCH (n)
WITH DISTINCT labels(n) AS nodeLabels, keys(n) AS properties
RETURN {labels: nodeLabels, properties: properties} AS output

"""

rel_properties_query = """
MATCH ()-[r]->()
WITH DISTINCT type(r) AS nodeLabels, collect(keys(r)) AS properties
RETURN {type: nodeLabels, properties: reduce(acc = [], prop in properties | acc + prop)} AS output
"""

rel_query = """
MATCH ()-[r]->()
WITH DISTINCT type(r) AS relationshipType, keys(r) AS properties
UNWIND properties AS property
RETURN {source: '', relationship: relationshipType, target: property} AS output
"""