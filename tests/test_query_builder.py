"""Behavior tests for QueryBuilder - validates actual query string output."""

import pytest


@pytest.mark.parametrize(
    "schema,select,expand,filter_expr,expected",
    [
        # Basic query with just select
        (
            "Basin",
            "basin_name,basin_id",
            None,
            None,
            "Basin?%24format=json&%24select=basin_name%2Cbasin_id",
        ),
        # Query with select and filter
        (
            "Well",
            "well_name,well_uwi",
            None,
            "well_name eq 'A-1'",
            "Well?%24format=json&%24select=well_name%2Cwell_uwi&%24filter=well_name+eq+%27A-1%27",
        ),
        # Query with select and expand
        (
            "Fault",
            "fault_id,fault_type",
            "interpretations",
            None,
            "Fault?%24format=json&%24select=fault_id%2Cfault_type&%24expand=interpretations",
        ),
        # Complete query with all parameters
        (
            "Wellbore",
            "wellbore_name,depth",
            "wells",
            "depth gt 1000",
            "Wellbore?%24format=json&%24select=wellbore_name%2Cdepth&%24expand=wells&%24filter=depth+gt+1000",
        ),
        # Query with numeric filter
        (
            "Basin",
            "basin_id,area",
            None,
            "area gt 500.5",
            "Basin?%24format=json&%24select=basin_id%2Carea&%24filter=area+gt+500.5",
        ),
    ],
)
def test_query_builder_produces_correct_format(
    schema, select, expand, filter_expr, expected
):
    """Test that QueryBuilder produces correctly formatted query strings for various combinations."""
    from src.dsis_client.api.query import QueryBuilder

    builder = QueryBuilder(district_id="TestDist", field="TestField")
    builder.schema(schema)

    if select:
        builder.select(select)
    if expand:
        builder.expand(expand)
    if filter_expr:
        builder.filter(filter_expr)

    query_string = builder.get_query_string()
    assert query_string == expected


def test_query_builder_reset_allows_reuse():
    """Test that reset clears all parameters allowing builder reuse."""
    from src.dsis_client.api.query import QueryBuilder

    builder = QueryBuilder(district_id="123", field="Field")
    builder.schema("Well").select("name").filter("depth gt 100")
    first_query = builder.get_query_string()

    # Reset and verify new query doesn't include previous parameters
    builder.reset()
    builder.schema("Basin").select("id")
    second_query = builder.get_query_string()

    assert first_query == "Well?%24format=json&%24select=name&%24filter=depth+gt+100"
    assert second_query == "Basin?%24format=json&%24select=id"
    assert "depth" not in second_query


def test_query_requires_schema():
    """Test that schema must be set before building query string."""
    from src.dsis_client.api.query import QueryBuilder

    builder = QueryBuilder(district_id="123", field="test")

    with pytest.raises(ValueError, match="schema must be set"):
        builder.get_query_string()
