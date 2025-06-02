import sdmx
import pandas as pd
from iso3166 import countries_by_alpha3, countries

client = sdmx.Client()

# ------------------ CONSTANTS ------------------
color_grid = {
    (1, 0): '#ffcccc', (1, 1): '#ff9999', (1, 2): '#ff6666', (1, 3): '#cc0000',
    (2, 0): '#ffffcc', (2, 1): '#ffff99', (2, 2): '#ffff66', (2, 3): '#cccc00',
    (3, 0): '#ccccff', (3, 1): '#9999ff', (3, 2): '#6666ff', (3, 3): '#0000cc'
}
iso3_to_country = {code: country.name for code, country in countries_by_alpha3.items()}
iso2_to_iso3 = {country.alpha2: country.alpha3 for country in countries}

# Source category: 1 → China, 2 → EU, 3 → US
source_mapping = {'CN': 1, 'B0': 2, 'US': 3}
partner_labels = {'B0': 'European Union', 'CN': 'China', 'US': 'United States'}

def bin_share(value):
    if pd.isna(value): return None
    if value <= 25: return 0
    elif value <= 50: return 1
    elif value <= 75: return 2
    else: return 3

# ------------------ HELPER ------------------
def get_highest_source(row):
    sources = {'B0': row.get('B0'), 'CN': row.get('CN'), 'US': row.get('US')}
    valid_sources = {k: v for k, v in sources.items() if pd.notna(v)}
    if not valid_sources:
        return None, None, None
    total = sum(valid_sources.values())
    max_key = max(valid_sources, key=valid_sources.get)
    max_value = valid_sources[max_key]
    return max_key, round((max_value / total) * 100, 1), round(max_value, 1)

# ------------------ MAIN FUNCTION ------------------
def imf_data(url):
    message = client.get(url=url)
    raw = sdmx.to_pandas(message.data[0])
    df = raw.reset_index()

    # Pivot COUNTERPART_AREA to columns
    df = df.pivot_table(index=['REF_AREA', 'TIME_PERIOD'], columns='COUNTERPART_AREA', values='value', aggfunc='first')
    df.reset_index(inplace=True)
    df.columns.name = None

    # Compute top import partner and share
    df[['Import Partner', 'Percent', 'Amount (USD Millions)']] = df.apply(lambda row: pd.Series(get_highest_source(row)), axis=1)

    # Split period
    df[['Year', 'Month']] = df['TIME_PERIOD'].str.split('-', expand=True)

    # Map import source to a source category and bin by share
    df['source_cat'] = df['Import Partner'].map(source_mapping)
    df['share_bin'] = df['Percent'].apply(bin_share)

    # Color key from (source, bin)
    df['color_key'] = df.apply(lambda row: color_grid.get((row['source_cat'], row['share_bin']), '#cccccc'), axis=1)

    # Convert REF_AREA (ISO2) to ISO3 for Plotly
    df['ISO3'] = df['REF_AREA'].map(iso2_to_iso3)
    df['CountryName'] = df['ISO3'].map(iso3_to_country).fillna(df['REF_AREA'])

    # Human-readable partner name
    df['Import Partner'] = df['Import Partner'].map(partner_labels)

    
    df_out = df[[
        'TIME_PERIOD', 'Year', 'Month', 'REF_AREA', 'ISO3', 'CountryName',
        'Import Partner', 'Percent', 'Amount (USD Millions)', 'color_key'
    ]]

    df_out = df_out.sort_values(by=['REF_AREA', 'TIME_PERIOD'])
    df_out['PercentChange'] = df_out.groupby('REF_AREA')["Amount (USD Millions)"].pct_change() * 100
    df_out['PercentChange'] = df_out['PercentChange'].round(1)

    return df_out


# ------------------ EXPORT ------------------
url = "http://dataservices.imf.org/REST/SDMX_XML.svc/CompactData/DOT/M..TMG_CIF_USD.US+CN+B0?startPeriod=2000&format=sdmx-2.1"
data = imf_data(url)
data = data.dropna(subset=['ISO3'])
data.to_json("data.json", orient="records", indent=2)

