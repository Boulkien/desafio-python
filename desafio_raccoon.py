import pandas as pd
import requests
import json

# nome, status, tipo, ano, dia mes
ticket_table = pd.read_json("https://us-central1-raccoon-bi.cloudfunctions.net/psel_de_ingressos")
# Nascent Letter, Symbolic Toy, Terrestrial Chair
show_table = requests.get("https://us-central1-raccoon-bi.cloudfunctions.net/psel_de_shows")
if show_table.status_code == 200:
    show_table = show_table.json()
# nome, show, gastos
buy_table = pd.read_csv("https://us-central1-raccoon-bi.cloudfunctions.net/psel_de_compras")

"""# Questão 1
## Média de gastos de pessoas com ingresso pista.
"""

print(ticket_table)

print(buy_table)

'''Neste caso, precisei fazer uma pequena manipulação para que o nome da banda deixasse de ser um índice de nossa tabela
e se tornasse uma coluna.

Exemplo:
index|ano |dia|mes              index|show|ano |dia|mes 
a    |2020|01 |01               0	   |a   |2020|01 |01
b    |2021|02 |02       ->      1    |b   |2021|02 |02
c    |2022|03 |03               2    |c   |2022|03 |03
'''
show_table_entries = []
for band_name, band_info in list(show_table.items()):
    show = {"show": band_name,
            "ano_show": band_info["ano"],
            "dia_show": band_info["dia"],
            "mes_show": band_info["mes"]}
    show_table_entries.append(show)
print(show_table_entries)

show_table = pd.DataFrame(show_table_entries)
print(show_table)

'''Relação de pessoas que compraram ingressos do tipo Pista.
  Aqui foi utilizado a coluna "Dia" para dar um merge entre a tabela de ingressos e a tabela dos Shows, para futuramente
  utilizar os nomes das bandas para relacionar os dados de gastos das pessoas na Pista por show.
[ ! ] Pessoas desta tabela podem ou não ter comparecido ao show.
'''
people_that_goes_for_lane = ticket_table[(ticket_table['status'] == 'Concluido') & (ticket_table['tipo'] == 'Pista')]
people_that_goes_for_lane = pd.merge(left=people_that_goes_for_lane, right=show_table, right_on='dia_show', left_on="dia", how="left")

merged_table = pd.merge(left=people_that_goes_for_lane, 
                        right=buy_table, 
                        on=["nome", "show"], 
                        how="left")
merged_table.fillna(0, inplace=True)
print(merged_table)

total_earned_money = merged_table['gastos'].sum()
earned_mean = total_earned_money / merged_table['gastos'].count()

print(f'Média de gastos: {float("%.2f" % earned_mean)}')

"""# QUOTE 2
## Quais pessoas não compareceram aos shows ?
"""

'''Relação de Pessoas que concluíram o pagamento com a Ticket Amazing
    Nessa seção, separei as pessoas com o status "Concluido" da tabela de Ingressos e logo após juntei com os dados da tabela de Show para adicionar as bandas na lista.
    Após esses merges e com os dados necessários, só restou juntar com a tabela de compras dando como chave de referência o "nome" e "show" que as pessoas foram, 
    assim, recolhendo os dados que continham como gasto "0" (utilizei o método fillna para adicionarmos aos valores que retornavam NaN) e finalizaqndo a questão.
'''
people_that_bought_ticket = ticket_table[ticket_table['status'] == 'Concluido']
people_that_bought_ticket = pd.merge(left=people_that_bought_ticket, 
                                     right=show_table,
                                     right_on='dia_show', 
                                     left_on="dia", 
                                     how="left")

people_who_went = pd.merge(left=people_that_bought_ticket,
                           right=buy_table,
                           on=["nome", "show"], 
                           how="left")
people_who_went.fillna(0, inplace=True)

print(people_who_went)

people_who_didnt_go = people_who_went[people_who_went['gastos'] == 0]
print("Pessoas que não compareceram ao show: ")
print(people_who_didnt_go)

"""# QUOTE 3  
## Quais pessoas compraram ingressos com concorrentes ?
"""

'''Relação de pessoas que compraram o ingresso com a AT e quem compareceu aos shows.
  Aqui separamos duas tabelas: Quem comprou ingresso com a AT e quem compareceu aos shows -, após juntarmos essas duas tabelas, foi apenas necessário excluir os dados que estavam presentes
  entre as duas tabelas e as que estavam presentes apenas na tabela de compra de ingresso, restando apenas que compareceu ao show mas não estava na tabela de ingressos da TA.
'''
people_that_bought_ticket_with_at = people_that_bought_ticket[["nome", "show"]]
people_that_bought_ticket_to_the_show_from_any_source = buy_table[["nome", "show"]]

print("Pessoas que compraram o ingresso através da AT: ")
print(people_that_bought_ticket_with_at)

people_that_bought_ticket_to_the_show_from_any_source = people_that_bought_ticket_to_the_show_from_any_source.drop_duplicates()
print("Pessoas que compraram o ingresso através de qualquer fonte: ")
print(people_that_bought_ticket_to_the_show_from_any_source)

people_that_doesnt_bought_ticket_from_at = pd.merge(left=people_that_bought_ticket_to_the_show_from_any_source, 
                                                    right=people_that_bought_ticket_with_at, 
                                                    on=["nome", "show"], 
                                                    how="outer", 
                                                    indicator=True)
people_that_doesnt_bought_ticket_from_at = people_that_doesnt_bought_ticket_from_at[people_that_doesnt_bought_ticket_from_at['_merge'] == 'left_only']

print("Pessoas que não compraram o ingresso através da AT: ")
print(people_that_doesnt_bought_ticket_from_at)

"""# QUOTE 4
## Qual o dia com o maior gasto ?
"""

'''Aqui foi necessário juntar a tabela de Show com a tabela de Gastos para sabermos o nome da banda que apresentou no dia x, depois foi só fazer a soma dos gastos por show e 
  criar uma função de soma para juntar os valores de cada dia e calcular qual dia as pessoas gastaram mais.
  
'''
list_of_spent_money_by_day = []
list_of_spent_money_by_show = pd.merge(left=show_table, 
                                       right=buy_table, 
                                       on="show", 
                                       how="left")
list_of_spent_money_by_show.fillna(0, inplace=True)

print("Lista de gastos por dia: ")
print(list_of_spent_money_by_show)

sum_of_spent_money_by_day = list_of_spent_money_by_show.groupby(['dia_show', 'show', 'ano_show', 'mes_show']).sum()
biggest_day_spent = max(sum_of_spent_money_by_day.iterrows(), key=lambda x: x[1]['gastos'])
print("Dia com o maior gasto:")
print(f"Dia: {biggest_day_spent[0][0]}, Dinheiro gasto: {biggest_day_spent[1].values[0]}")

"""# QUOTE 5
## Faça uma lista com os clientes que desistiram de comprar o ingresso com a AT, a soma do valor que foi gasto durante os shows e quais shows eles desistiram de comprar. 
"""

# Lista das pessoas que tiveram algum problema no pagamento.

# Problemas no Pagamento
tickets_with_payment_problems = ticket_table[(ticket_table['status'] == 'Problema no Pagamento') | (ticket_table['status'] == 'Nao Concluido')]
tickets_with_payment_problems = tickets_with_payment_problems[["dia", "nome", "status"]]
print(tickets_with_payment_problems)

# Pessoas que concluíram o pagamento

# Pagamentos Concluidos
tickets_with_payment_success = ticket_table[ticket_table['status'] == 'Concluido']
tickets_with_payment_success = tickets_with_payment_success[["dia", "nome", "status"]]

''' Nesta tabela juntei as duas tabelas anteriores, e como havia pessoas que mesmo após terem problemas com o pagamento conseguiram completa-lo, foi necessário fazer um match com o nome e o dia 
  que a pessoa conseguiu fazer a compra e apenas deixar as pessoas que não conseguiram fazer a compra e não tinha dados de que conseguiram posteriormente.

'''

# Pessoas que desistiram
dropout = pd.merge(left= tickets_with_payment_problems, 
                   right=tickets_with_payment_success, 
                   on=["nome", "dia"], 
                   how="outer", 
                   indicator=True)
dropout = dropout[dropout['_merge'] == 'left_only']
dropout = dropout[["nome", "dia","status_x" ]]

print("Pessoas que desistiram: ")
print(dropout)

''' Aqui foi necessário juntar a tabela anterior com a tabela de shows para podermos ter a base dos dias do show junto com o nome das bandas que se apresentaram no dia, para depois saber a quantia
  gasta por show na tabela de compras. foi necessário retirar os dados duplicados das tentivas de compra de ingresso.
'''
# Pessoas que desistiram e dias do show
dropout_and_show = pd.merge(left=dropout, 
                            right=show_table, 
                            right_on='dia_show', 
                            left_on="dia", 
                            how="left")
dropout_and_show = dropout_and_show[["nome","dia", "show", "status_x"]]

#Organizando Dados
dropout_and_show = dropout_and_show.drop_duplicates()

''' Nesta tabela, juntei os dados das pessoas que não compraram ingresso pela AT mas apresentava algum gasto nos dias dos shows, após o merge, foi preciso foi necessário somar os gastos totais nos shows,
  e formatar um arquivo JSON para apresentar os dados conforme o enunciado.
'''

# Pessoas que desistiram de comprar com a AT e gastos
dropout_cost = pd.merge(left=dropout_and_show, 
                        right=buy_table, 
                        on=["nome", "show"], 
                        how="left")
dropout_cost.fillna(0, inplace=True)
grouped = dropout_cost.groupby(["nome", "show", "dia"]).sum()

dict_of_spent_money_by_people = {}

for labels, money_spent in grouped.iterrows():
    spent_money_val = money_spent.values[0]
    name, show_name, day = labels
    
    if dict_of_spent_money_by_people.get(name) is None:
        dict_of_spent_money_by_people[name] = {
            "nome": name,
            "gastos": spent_money_val,
            "shows": [show_name]
        }
    else:
        dict_of_spent_money_by_people[name]["gastos"] += spent_money_val
        dict_of_spent_money_by_people[name]["shows"].append(show_name)

output_list = []

for person in dict_of_spent_money_by_people:
    output_list.append(dict_of_spent_money_by_people[person])

jsonified = json.dumps(output_list, indent=4) # [ ! ] Versão JSON respeitando o modelo imposto no enunciado da questão.
print(pd.DataFrame(output_list))