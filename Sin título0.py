import gluestick as gs
#import pandas as pd
#import os
<<<<<<< Updated upstream
#import ast ###Pull request##
=======
#import ast ###Prueba pull request###asdasdsada
>>>>>>> Stashed changes
#import locale

ROOT_DIR = os.environ.get("ROOT_DIR", ".")
INPUT_DIR = f"{ROOT_DIR}/sync-output"
OUTPUT_DIR = f"{ROOT_DIR}/etl-output"


input_data = gs.read_csv_folder(INPUT_DIR)

for key in input_data:
    input_df = input_data[key]
    #print(input_df[['id','customer_name','customer_email']]) 
    
 #Selecciono Campos
invoices = input_df[['id', 'customer_name', 'customer_email', 'amount_remaining', 'number']]
 #Renombro Campos y Quito los duplicados
invoices = invoices.pipe(lambda x: x.rename(columns={'customer_name': 'Nombre', 'customer_email': 'Mail', 
                                                     'amount_remaining' : 'Monto_Pendiente', 'number': 'Numero_Factura'})).drop_duplicates()
#invoices.astype({"Monto_Pendiente" : float})
#print(invoices["Monto_Pendiente"].apply(type))
for i in invoices.index:
    invoices['Monto_Pendiente'][i] = "{:0,.2f}".format(float(invoices['Monto_Pendiente'][i]))

invoices.to_csv(f"{OUTPUT_DIR}/invoices.csv", index=False)
print(invoices.head(2))
#invoices.to_csv(f"{OUTPUT_DIR}/invoices.csv", index=False)