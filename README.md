# ontology-kp
NCATS Data Translator Knowledge Provider for ontology triples

TRAPI (Translator Reasoner API) wrapper for Ubergraph.

## TRAPI extensions

When ontology-kp queries Ubergraph, it by default matches edges from the "redundant" graph; that is, the logically complete set of inferrable relations. Using an attribute constraint on an edge, you can restrict that edge to a "direct" relationship (querying Ubergraph's nonredundant graph). In this way you can ask for immediate superclasses/subclass, rather than the logically complete set. Here edge `e0` is constrained to be direct.

```json
{
  "message": {
    "query_graph": {
      "nodes": {
        "n0": {
          "categories": [
            "biolink:BiologicalProcess"
          ],
          "is_set": false
        },
        "n1": {
          "categories": [
            "biolink:BiologicalProcess"
          ],
          "is_set": false
        }
      },
      "edges": {
        "e0": {
          "predicates": [
            "biolink:part_of"
          ],
          "subject": "n1",
          "object": "n0",
          "attribute_constraints": [
             {
              "id": "EDAM-DATA:2589",
              "name": "hierarchy",
              "operator": "==",
              "value": "direct"
             }
           ]
        }
      }
    }
  }
}
```
