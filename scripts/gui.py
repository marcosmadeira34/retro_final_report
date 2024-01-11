import tkinter as tk
import customtkinter as ctk

# Cria uma janela
root = ctk.CTk()
root.title("Relatório de Faturamento")
root.geometry("800x400")

# Adicione aqui os elementos da interface




button_check_and_update_orders = ctk.CTkButton(
                                root, 
                                text="VERIFICAR NOVOS PEDIDOS", 
                                font=('Lato Regular', 20, 'bold'),
                                width=480,
                                height=50,
                                border_width=2,
                                corner_radius=5)

button_check_and_update_orders.place(relx=0.5, rely=0.5, anchor=tk.CENTER)

# Inicia o loop principal da aplicação
root.mainloop()
