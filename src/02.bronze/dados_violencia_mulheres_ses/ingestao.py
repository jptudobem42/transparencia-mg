#%%

import sys
sys.path.insert(0, '..\..\..\lib')
from ingestors import IngestionBronze
        
#%%
area = 'dados_violencia_mulheres_ses'
api_url = f'https://dados.mg.gov.br/api/3/action/package_show?id={area}'
output_dir = '.\data'

ingest = IngestionBronze(api_url)
ingest.get_and_save(output_dir)
