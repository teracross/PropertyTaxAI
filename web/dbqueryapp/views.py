from django.shortcuts import render
from django.db import connection
from .forms import QueryForm

SQL_QUERIES = {
    'get_first_100_real_acct': "SELECT * FROM real_acct LIMIT 100",
    'get_avg_bldg_and_land_val_by_state_class': "SELECT state_class, AVG(CAST(bld_val AS numeric)) AS avg_building_value, AVG(CAST(land_val AS numeric)) AS avg_land_value FROM real_acct GROUP BY state_class;",
    'get_first_100_unique_owners_for_residential':"SELECT b.acct, COUNT(DISTINCT o.name) AS distinct_owner_count, STRING_AGG(DISTINCT o.name, ', ') AS owner_names FROM building_res b JOIN ownership_history o ON b.acct = o.acct WHERE b.acct IN (SELECT acct FROM building_res LIMIT 100) GROUP BY b.acct;",

}

def dictfetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

def home(request):
    result = None
    error = None
    form = QueryForm(request.POST or None)
    if request.method == 'POST' and form.is_valid():
        query_key = form.cleaned_data['query']
        sql = SQL_QUERIES.get(query_key)
        if not sql:
            error = 'No SQL query found for the selected option.'
        else: 
            try:
                with connection.cursor() as cursor:
                    cursor.execute(sql)
                    result = dictfetchall(cursor)
            except Exception as e:
                error = str(e)
    return render(request, 'home.html', {'form': form, 'result': result, 'error': error})
