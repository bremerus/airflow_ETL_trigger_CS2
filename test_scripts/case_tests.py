import pandas as pd
import time

# Test for the collumn gender 
start_time = time.perf_counter()

lista = []

for index, row in df.iterrows():
    if row['gender'] not in lista:
        lista.append(row['gender'])

end_time = time.perf_counter()
total_time = end_time - start_time
print(lista)

print(f'It took {total_time} time')

start_time = time.perf_counter()

lista2 = df['gender'].unique().tolist()
print(lista2)
end_time = time.perf_counter()
total_time = end_time - start_time
print(f'It took {total_time} time')


# test for senior citizens

lista2 = df['senior_citizen'].unique().tolist()
print(lista2)

df['senior_citizen'] = df['senior_citizen'].astype(object)



for index, row in df.iterrows():
    value = row['senior_citizen']

    if pd.isna(value):
        continue

    value = str(value)
    if value == '0' or value == '1':
        df.loc[index, 'senior_citizen'] = int(value)
    else:
        if value == 'No' :
            df.loc[index, 'senior_citizen'] = 0
        else:
            df.loc[index, 'senior_citizen'] = 1

lista2 = df['senior_citizen'].unique().tolist()
print(lista2)


# tenure testing


df['tenure'] = pd.to_numeric(df['tenure'], errors='coerce')

df['tenure'] = df['tenure'].astype('Int64')


lista2 = df['tenure'].unique().tolist()

#multiple_lines test


lista2 = df['multiple_lines'].unique().tolist()
print(lista2)

df['multiple_lines'] = df['multiple_lines'].str.strip().str.lower().replace({'yes' : 1, 'y' : 1, 'no' : 0, 'n' : 0, 'no phone service' : 0})


lista2 = df['multiple_lines'].unique().tolist()
print(lista2)


# online_security test

lista2 = df['online_security'].unique().tolist()
print(lista2)

df['online_security'] = df['online_security'].str.strip().str.lower().replace({'yes' : 1, 'y' : 1, 'no' : 0, 'n' : 0, 'no internet service' : 0})

lista2 = df['online_security'].unique().tolist()