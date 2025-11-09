from django import forms

QUERY_CHOICES = [
    ('get_first_100_real_acct', 'First 100 real accounts from the real_acct table.'),
    ('get_avg_bldg_and_land_val_by_state_class', ' Average building and land value by stateclass.'),
    ('get_first_100_unique_owners_for_residential', ' Get residential account unique owner history.'),
]

class QueryForm(forms.Form):
    query = forms.ChoiceField(choices=QUERY_CHOICES, widget=forms.Select(attrs={'class': 'query-dropdown'}))
