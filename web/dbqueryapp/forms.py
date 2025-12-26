from django import forms

QUERY_CHOICES = [
    ('get_first_100_real_acct', 'First 100 real accounts from the real_acct table.'),
    ('get_avg_bldg_and_land_val_by_state_class', ' Average building and land value by stateclass.'),
    ('get_first_100_unique_owners_for_residential', ' Get residential account unique owner history.'),
]


class QueryForm(forms.Form):
    form_type = forms.CharField(widget=forms.HiddenInput(), initial='query_form')
    query = forms.ChoiceField(choices=QUERY_CHOICES, widget=forms.Select(attrs={'class': 'query-dropdown'}))


class CustomSQLForm(forms.Form):
    form_type = forms.CharField(widget=forms.HiddenInput(), initial='custom_sql_form')
    user_sql = forms.CharField(
        required=False,
        label='Custom SQL (read-only)',
        widget=forms.Textarea(attrs={
            'rows': 6,
            'placeholder': 'Enter a read-only SELECT or WITH query here (LIMIT will be applied).',
            'style': 'width:100%; padding:8px; border-radius:6px; border:1px solid #ddd;'
        })
    )
