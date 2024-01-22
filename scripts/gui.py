import tkinter as tk
import customtkinter as ctk


ctk.set_appearance_mode("dark")


class App:
    def __init__(self, root):
        self.root = root
        self.root.title("Menu Lateral")

        # Frame principal
        self.main_frame = ctk.CTkFrame(self.root)
        self.main_frame.pack(fill=tk.BOTH, expand=True)
        self.root.geometry("1200x720")

               
        # Frame do cabeçalho
        self.header_frame = ctk.CTkFrame(self.main_frame)
        self.header_frame.pack(side=tk.TOP, fill=tk.X)

        
        # Adicionar opções ao menu
        self.add_menu_option(self.header_frame, "Verificar Pedidos", self.verificar_pedidos)
        self.add_menu_option(self.header_frame, "Baixar Pedidos", self.baixar_pedidos)


        # Adicona rótulo ao frame principal
        self.label = ctk.CTkLabel(self.main_frame, text='', font=("Arial", 18))
        self.label.pack(side=tk.TOP, padx=600, pady=10)    


    def add_menu_option(self, frame, text, command):
        option_button = ctk.CTkButton(frame, text=text, command=lambda: self.show_message(text))
        option_button.pack(side=tk.LEFT, padx=5, pady=5)

    def verificar_pedidos(self):
        print("Opção: Verificar Pedidos")
        self.show_message("Verificar Pedidos")

    def baixar_pedidos(self):
        print("Opção: Baixar Pedidos")
        self.show_message("Baixar Pedidos")

    

    def show_message(self, message):
        self.label.configure(text=message)

if __name__ == "__main__":
    root = tk.Tk()  # Usando a classe Tk do módulo tkinter
    app = App(root)
    root.mainloop()