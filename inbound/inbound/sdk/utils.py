from jinja2 import Environment


def get_query_list(query_template: str, highwatermarks: list[dict] = [{}]) -> list[str]:
    jinja_template = Environment().from_string(source=query_template)
    queries = []
    for highwatermark in highwatermarks:
        queries.append(jinja_template.render(highwatermark=highwatermark))
    return queries
