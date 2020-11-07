# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import json

with open('opetushallitus_dump.json', 'r', encoding="utf-8") as json_file:
   json_dict = json.load(json_file)

# Parse Projects; F_HAUT
data = []
all_keys = []
for haku in json_dict['loppuselvitys']:
    org_name = haku['organization_name']
    proj_name = haku['project_name']
    answers = haku['loppuselvitys_answers']['value']
    keys = ['organization_name','project_name', 'haku_id']
    values = [org_name,proj_name, haku['haku_id']]
    for answer in answers:
        key = answer['key']
        value = answer['value']
        if isinstance(value,list):
            try:
                for jj in value:
                    iterate=True
                    n_key = jj['key']
                    n_value = jj['value']
                    while iterate:
                        if isinstance(n_value,list):
                            prev_list_of_dicts = n_value
                            n_value = n_value[0]['value']    
                        else:
                            iterate = False
                    for i in prev_list_of_dicts:
                        key = i['key']
                        value = i['value']
                        keys.append(key)
                        values.append(value)
            except:
                try:
                    value = ','.join(value)
                except:
                    value = str(value)
        else:
            keys.append(key)
            values.append(value)
    df = pd.DataFrame(values,index=keys).T
    data.append(df)
        
all_dfs = pd.concat(data)
df_long = pd.melt(all_dfs, id_vars=['haku_id','organization_name','project_name'])
df_long.rename(columns={'variable':'variable_id'}, inplace=True)
df_long.dropna(subset = ['value'], inplace=True)
df_long.to_csv('F_HAUT.csv', sep ='$', index=False)

#%% D_HANKKEET
keys = ['Haku_id', 'Nimi', 'Haun alkamispvm', 'Haun loppumispvm', 'Painopistealuuet', 'Omarahoitusosuus']
values = []
for hanke in json_dict['haku']:
    # Haku_ID
    id = hanke['id']
    # Nimi
    name = hanke['content']['name']['fi']
    # Haun alkamispvm
    start = hanke['content']['duration']['start']
    # Haun loppumispvm
    end = hanke['content']['duration']['end']
    # Painopistealuuet
    painopisteet = hanke['content']['focus-areas']['items']
    if len(painopisteet)>0:
        painopiste = ''
        for painopiste_tmp in painopisteet:
            painopiste = painopiste_tmp['fi'] + '. '+ painopiste
    # Omarahoitusosuus
    omarahoitus = hanke['content'].get('self-financing-percentage')
    values.append([id, name, start, end, painopiste, omarahoitus])

D_HANKKEET = pd.DataFrame(values)
D_HANKKEET.columns = keys
D_HANKKEET.to_csv('D_HANKKEET.csv', sep = '$', index=False)
    
#%% D_VARIABLES
def flatten_json(y):
    # Recursively parse highly nested JSON
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x
    flatten(y)
    return out    
interesting_values = all_dfs.columns.to_list()
dfs = []
for hanke in json_dict['haku'] :
    loppuselvitys_form = hanke['loppuselvitys_form']
    id = hanke['id']
    for loppu in loppuselvitys_form:
        out = flatten_json(loppu)   
        D_LOPPUSELVITYS_tmp = pd.DataFrame()
        for key,value in out.items():
            if value in interesting_values:
                D_LOPPUSELVITYS_tmp['variable_id'] = [value]
                level_name = out.get('label_fi')
                level=key.count('children')
                if not level_name:
                    D_LOPPUSELVITYS_tmp[f'level_0'] = out.get('helpText_fi')
                else:
                    D_LOPPUSELVITYS_tmp[f'level_0'] = out.get('label_fi')
                D_LOPPUSELVITYS_tmp['haku_id'] = id
                if level > 1:
                    level_name_key = key[:-13]+'label_fi'
                    try:
                        level_name = out[level_name_key]
                    except:
                        level_name = out.get((key[:-2]+'label_fi'))
                    D_LOPPUSELVITYS_tmp[f'level_{level}'] = level_name
                    level -= 1
                    while level >= 0:
                        level_name_key = level_name_key[:-19]+'label_fi'
                        level_name = out.get(level_name_key)
                        D_LOPPUSELVITYS_tmp[f'level_{level}'] = level_name
                        level -= 1                
                elif level == 1:
                    level_name_key = key[:-2]+'label_fi'
                    level_name = out[level_name_key]
                    D_LOPPUSELVITYS_tmp[f'level_{level}'] = level_name
                dfs.append(D_LOPPUSELVITYS_tmp.copy(deep=True))

D_LOPPUSELVITYS = pd.concat(dfs)
D_LOPPUSELVITYS.to_csv('D_VARIABLES.csv', sep = '$', index=False)

import re
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer

# Tokenize words and get common word count to get insight into the projects
stemmer = SnowballStemmer("finnish")

F_HAUT = pd.read_csv('F_HAUT.csv', sep='$')

fin_stopwords = {"0","1","2","3","4","5","6","value","key","textfield","fieldtype","aiemmin","aika","aikaa","aikaan","aikaisemmin","aikaisin","aikajen","aikana","aikoina","aikoo","aikovat","aina","ainakaan","ainakin","ainoa","ainoat","aiomme","aion","aiotte","aist","aivan","ajan","alas","alemmas","alkuisin","alkuun","alla","alle","aloitamme","aloitan","aloitat","aloitatte","aloitattivat","aloitettava","aloitettevaksi","aloitettu","aloitimme","aloitin","aloitit","aloititte","aloittaa","aloittamatta","aloitti","aloittivat","alta","aluksi","alussa","alusta","annettavaksi","annetteva","annettu","ansiosta","antaa","antamatta","antoi","aoua","apu","asia","asiaa","asian","asiasta","asiat","asioiden","asioihin","asioita","asti","avuksi","avulla","avun","avutta","edelle","edelleen","edellä","edeltä","edemmäs","edes","edessä","edestä","ehkä","ei","eikä","eilen","eivät","eli","ellei","elleivät","ellemme","ellen","ellet","ellette","emme","en","enemmän","eniten","ennen","ensi","ensimmäinen","ensimmäiseksi","ensimmäisen","ensimmäisenä","ensimmäiset","ensimmäisiksi","ensimmäisinä","ensimmäisiä","ensimmäistä","ensin","entinen","entisen","entisiä","entisten","entistä","enää","eri","erittäin","erityisesti","eräiden","eräs","eräät","esi","esiin","esillä","esimerkiksi","et","eteen","etenkin","etessa","ette","ettei","että","haikki","halua","haluaa","haluamatta","haluamme","haluan","haluat","haluatte","haluavat","halunnut","halusi","halusimme","halusin","halusit","halusitte","halusivat","halutessa","haluton","he","hei","heidän","heidät","heihin","heille","heillä","heiltä","heissä","heistä","heitä","helposti","heti","hetkellä","hieman","hitaasti","hoikein","huolimatta","huomenna","hyvien","hyviin","hyviksi","hyville","hyviltä","hyvin","hyvinä","hyvissä","hyvistä","hyviä","hyvä","hyvät","hyvää","hän","häneen","hänelle","hänellä","häneltä","hänen","hänessä","hänestä","hänet","häntä","ihan","ilman","ilmeisesti","itse","itsensä","itseään","ja","jo","johon","joiden","joihin","joiksi","joilla","joille","joilta","joina","joissa","joista","joita","joka","jokainen","jokin","joko","joksi","joku","jolla","jolle","jolloin","jolta","jompikumpi","jona","jonka","jonkin","jonne","joo","jopa","jos","joskus","jossa","josta","jota","jotain","joten","jotenkin","jotenkuten","jotka","jotta","jouduimme","jouduin","jouduit","jouduitte","joudumme","joudun","joudutte","joukkoon","joukossa","joukosta","joutua","joutui","joutuivat","joutumaan","joutuu","joutuvat","juuri","jälkeen","jälleen","jää","kahdeksan","kahdeksannen","kahdella","kahdelle","kahdelta","kahden","kahdessa","kahdesta","kahta","kahteen","kai","kaiken","kaikille","kaikilta","kaikkea","kaikki","kaikkia","kaikkiaan","kaikkialla","kaikkialle","kaikkialta","kaikkien","kaikkin","kaksi","kannalta","kannattaa","kanssa","kanssaan","kanssamme","kanssani","kanssanne","kanssasi","kauan","kauemmas","kaukana","kautta","kehen","keiden","keihin","keiksi","keille","keillä","keiltä","keinä","keissä","keistä","keitten","keittä","keitä","keneen","keneksi","kenelle","kenellä","keneltä","kenen","kenenä","kenessä","kenestä","kenet","kenettä","kennessästä","kenties","kerran","kerta","kertaa","keskellä","kesken","keskimäärin","ketkä","ketä","kiitos","kohti","koko","kokonaan","kolmas","kolme","kolmen","kolmesti","koska","koskaan","kovin","kuin","kuinka","kuinkan","kuitenkaan","kuitenkin","kuka","kukaan","kukin","kukka","kumpainen","kumpainenkaan","kumpi","kumpikaan","kumpikin","kun","kuten","kuuden","kuusi","kuutta","kylliksi","kyllä","kymmenen","kyse","liian","liki","lisäksi","lisää","lla","luo","luona","lähekkäin","lähelle","lähellä","läheltä","lähemmäs","lähes","lähinnä","lähtien","läpi","mahdollisimman","mahdollista","me","meidän","meidät","meihin","meille","meillä","meiltä","meissä","meistä","meitä","melkein","melko","menee","meneet","menemme","menen","menet","menette","menevät","meni","menimme","menin","menit","menivät","mennessä","mennyt","menossa","mihin","mikin","miksi","mikä","mikäli","mikään","mille","milloin","milloinkan","millä","miltä","minkä","minne","minua","minulla","minulle","minulta","minun","minussa","minusta","minut","minuun","minä","missä","mistä","miten","mitkä","mitä","mitään","moi","molemmat","mones","monesti","monet","moni","moniaalla","moniaalle","moniaalta","monta","muassa","muiden","muita","muka","mukaan","mukaansa","mukana","mutta","muu","muualla","muualle","muualta","muuanne","muulloin","muun","muut","muuta","muutama","muutaman","muuten","myöhemmin","myös","myöskin","myöskään","myötä","ne","neljä","neljän","neljää","niiden","niihin","niiksi","niille","niillä","niiltä","niin","niinä","niissä","niistä","niitä","noiden","noihin","noiksi","noilla","noille","noilta","noin","noina","noissa","noista","noita","nopeammin","nopeasti","nopeiten","nro","nuo","nyt","näiden","näihin","näiksi","näille","näillä","näiltä","näin","näinä","näissä","näissähin","näissälle","näissältä","näissästä","näistä","näitä","nämä","ohi","oikea","oikealla","oikein","ole","olemme","olen","olet","olette","oleva","olevan","olevat","oli","olimme","olin","olisi","olisimme","olisin","olisit","olisitte","olisivat","olit","olitte","olivat","olla","olleet","olli","ollut","oma","omaa","omaan","omaksi","omalle","omalta","oman","omassa","omat","omia","omien","omiin","omiksi","omille","omilta","omissa","omista","on","onkin","onko","ovat","paikoittain","paitsi","pakosti","paljon","paremmin","parempi","parhaillaan","parhaiten","perusteella","peräti","pian","pieneen","pieneksi","pienelle","pienellä","pieneltä","pienempi","pienestä","pieni","pienin","poikki","puolesta","puolestaan","päälle","runsaasti","saakka","sadam","sama","samaa","samaan","samalla","samallalta","samallassa","samallasta","saman","samat","samoin","sata","sataa","satojen","se","seitsemän","sekä","sen","seuraavat","siellä","sieltä","siihen","siinä","siis","siitä","sijaan","siksi","sille","silloin","sillä","silti","siltä","sinne","sinua","sinulla","sinulle","sinulta","sinun","sinussa","sinusta","sinut","sinuun","sinä","sisäkkäin","sisällä","siten","sitten","sitä","ssa","sta","suoraan","suuntaan","suuren","suuret","suuri","suuria","suurin","suurten","taa","taas","taemmas","tahansa","tai","takaa","takaisin","takana","takia","tallä","tapauksessa","tarpeeksi","tavalla","tavoitteena","te","teidän","teidät","teihin","teille","teillä","teiltä","teissä","teistä","teitä","tietysti","todella","toinen","toisaalla","toisaalle","toisaalta","toiseen","toiseksi","toisella","toiselle","toiselta","toisemme","toisen","toisensa","toisessa","toisesta","toista","toistaiseksi","toki","tosin","tuhannen","tuhat","tule","tulee","tulemme","tulen","tulet","tulette","tulevat","tulimme","tulin","tulisi","tulisimme","tulisin","tulisit","tulisitte","tulisivat","tulit","tulitte","tulivat","tulla","tulleet","tullut","tuntuu","tuo","tuohon","tuoksi","tuolla","tuolle","tuolloin","tuolta","tuon","tuona","tuonne","tuossa","tuosta","tuota","tuotä","tuskin","tykö","tähän","täksi","tälle","tällä","tällöin","tältä","tämä","tämän","tänne","tänä","tänään","tässä","tästä","täten","tätä","täysin","täytyvät","täytyy","täällä","täältä","ulkopuolella","usea","useasti","useimmiten","usein","useita","uudeksi","uudelleen","uuden","uudet","uusi","uusia","uusien","uusinta","uuteen","uutta","vaan","vahemmän","vai","vaiheessa","vaikea","vaikean","vaikeat","vaikeilla","vaikeille","vaikeilta","vaikeissa","vaikeista","vaikka","vain","varmasti","varsin","varsinkin","varten","vasen","vasenmalla","vasta","vastaan","vastakkain","vastan","verran","vielä","vierekkäin","vieressä","vieri","viiden","viime","viimeinen","viimeisen","viimeksi","viisi","voi","voidaan","voimme","voin","voisi","voit","voitte","voivat","vuoden","vuoksi","vuosi","vuosien","vuosina","vuotta","vähemmän","vähintään","vähiten","vähän","välillä","yhdeksän","yhden","yhdessä","yhteen","yhteensä","yhteydessä","yhteyteen","yhtä","yhtäälle","yhtäällä","yhtäältä","yhtään","yhä","yksi","yksin","yksittäin","yleensä","ylemmäs","yli","ylös","ympäri","älköön","älä"}
all_stopwords = set(stopwords.words(['english','finnish','swedish']))

word_counts = F_HAUT.fillna(-1).groupby(['haku_id', 'project_name','organization_name'])['value'].apply(lambda x: x.str.lower().str.split(expand=True).stack().value_counts())

word_counts = word_counts.reset_index()
word_counts['level_3'] = word_counts['level_3'].map(lambda x: stemmer.stem(re.sub(r'\W+', '', x)))
word_counts = word_counts[~word_counts['level_3'].isin(all_stopwords)]
word_counts = word_counts[~word_counts['level_3'].isin(fin_stopwords)]
word_counts = word_counts.groupby(['haku_id', 'project_name','organization_name','level_3'])['value'].sum().reset_index()

word_counts.to_csv('word_counts.csv', index=False, sep='$')

