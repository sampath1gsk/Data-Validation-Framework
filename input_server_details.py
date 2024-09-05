import tkinter as tk
from tkinter import ttk
from tkinter import messagebox

# Define global variable to store the result
global_result = {}

def is_positive_whole_number(value):
    return value.isdigit() and int(value) >= 0

def update_source_fields(*args):
    source = source_var.get()
    clear_fields(source_frame)
    
    source_fields_mapping = {
        "SQLSERVER": [("server_name", True), ("username", False), ("password", False)],
        "SNOWFLAKE": [("account", True), ("warehouse", True),("username", True), ("password", True) ],
        "FILE": []
    }

    if source_fields_mapping.get(source):
        source_frame.grid(row=2,column=2)
        for idx, (field, mandatory) in enumerate(source_fields_mapping.get(source, []), start=1):
            add_field(f"source_{field}", source_frame, idx, show='*' if field == "password" else None, mandatory=mandatory)
        if source == "SQLSERVER":
            messagebox.showinfo("To Enable Windows Authentication","Ensure that the 'username' and 'password' fields are left empty.")
    else:
        source_frame.grid_remove()

def update_target_fields(*args):
    target = target_var.get()
    clear_fields(target_frame)
    
    target_fields_mapping = {
        "SQLSERVER": [("server_name", True), ("username", False), ("password", False)],
        "SNOWFLAKE": [("account", True), ("warehouse", True),("username", True), ("password", True) ],
        "FILE": []
    }
    if target_fields_mapping.get(target):
        target_frame.grid(row=4,column=2)
        for idx, (field, mandatory) in enumerate(target_fields_mapping.get(target, []), start=1):
            add_field(f"target_{field}", target_frame, idx, show='*' if field == "password" else None, mandatory=mandatory)
        if target == "SQLSERVER":
            messagebox.showinfo("To Enable Windows Authentication","Ensure that the 'username' and 'password' fields are left empty.")
    else:
        target_frame.grid_remove()

def update_output_fields(*args):
    output = output_var.get()
    clear_fields(output_frame)
    output_fields_mapping = {
        "SQLSERVER": [("server_name", True), ("username", False), ("password", False),("database", True),("schema",True),("table_name",True),("Number of error records to be displayed",True)],
       "SNOWFLAKE": [("account", True), ("warehouse", True),("username", True), ("password", True),("database", True),("schema",True),("table_name",True),("Number of error records to be displayed",True) ],
        "CSV": [("Number of error records to be displayed",True)]
    }
    
    if output_fields_mapping.get(output):
        output_frame.grid(row=6,column=2)
        for idx, (field, mandatory) in enumerate(output_fields_mapping.get(output, []), start=1):
            add_field(f"output_{field}", output_frame, idx, show='*' if field == "password" else None, mandatory=mandatory)
        if output == "SQLSERVER":
            messagebox.showinfo("To Enable Windows Authentication","Ensure that the 'username' and 'password' fields are left empty.")
    else:
        output_frame.grid_remove()


def clear_fields(frame):
    global fields
    for widget in frame.winfo_children():
        widget.destroy()
    fields = {key: value for key, value in fields.items() if value[0].master != frame}

def trim_input(entry):
    value = entry.get().strip()
    entry.delete(0, tk.END)
    entry.insert(0, value)

def add_field(label_text, frame, row, show=None, mandatory=False):
    label = ttk.Label(frame, text=label_text, style="Custom.TLabel", width=20, anchor='e',wraplength=150)
    label.grid(column=0, row=row, padx=5, pady=5)
    entry = ttk.Entry(frame, show=show, style="Custom.TEntry") if show else ttk.Entry(frame, style="Custom.TEntry")
    entry.grid(column=1, row=row, padx=5, pady=5)
    
    if label_text.endswith("_password"):
        show_password = tk.BooleanVar()
        show_password.set(False)
        
        def toggle_password_visibility():
            if show_password.get():
                entry.config(show="")
            else:
                entry.config(show="*")
        
        checkbox = ttk.Checkbutton(frame, text="Show Password", variable=show_password, command=toggle_password_visibility)
        checkbox.grid(column=2, row=row, padx=5, pady=5)  

    entry.bind("<FocusOut>", lambda e: trim_input(entry))
    fields[label_text] = (entry, mandatory)
def submit_form():
    global global_result
    if not source_var.get():
        messagebox.showwarning("Mandatory Field", "Please select a Source Type")
        return
    if not target_var.get():
        messagebox.showwarning("Mandatory Field", "Please select a Target Type")
        return
    if not output_var.get():
        messagebox.showwarning("Mandatory Field", "Please select an Output Type")
        return

    for key, (entry, mandatory) in fields.items():
        if mandatory and entry.get() == "":
            messagebox.showwarning("Mandatory Field", f"Please fill in the field: {key}")
            return
        if key.endswith("Number of error records to be displayed") and not is_positive_whole_number(entry.get()):
            messagebox.showwarning("Invalid Input", f"Please enter a whole number for the field : {key}")
            return


    global_result = {
        "source_type": source_var.get(),
        "target_type": target_var.get(),
        "output_type": output_var.get()
    }
    global_result.update({key: entry.get() for key, (entry, _) in fields.items()})
    root.destroy()

# To Create the main window
root = tk.Tk()
root.title("Server Details")
root.configure(bg="#ffffff")  



# Define the variables
source_var = tk.StringVar()
target_var = tk.StringVar()
output_var = tk.StringVar()
fields = {}

# Create a style
style = ttk.Style()
style.configure("Custom.TLabel", foreground="#000000", background="#ffffff", font=("Helvetica", 10, "bold"))
style.configure("Custom.TEntry", foreground="#000000", background="#ffffff", font=("Helvetica", 10))
style.configure("Custom.TButton", foreground="#000000", background="#ffffff", font=("Helvetica", 10, "bold"))
style.map("Custom.TButton",
          background=[('active', '#ffffff'), ('disabled', '#ffffff')],
          foreground=[('disabled', '#666666')])

# Add a title label
title_label = ttk.Label(root, text="Data Validation Framework", style="Custom.TLabel", background="#ffffff", font=("Helvetica", 16, "bold"))
title_label.grid(column=2, row=0, columnspan=2, padx=5, pady=5)

# Create the dropdown menus and frames for dynamic fields

source_label = ttk.Label(root, text="Source Type:", style="Custom.TLabel", background="#ffffff")
source_label.grid(column=0, row=1, padx=5, pady=5)
source_menu = ttk.Combobox(root, textvariable=source_var)
source_menu['values'] = ["SQLSERVER", "SNOWFLAKE", "FILE"]
source_menu.grid(column=1, row=1, padx=5, pady=5)

source_frame = ttk.Frame(root, style="Custom.TFrame")
source_frame.grid(column=0, row=2, columnspan=2, padx=5, pady=5)

target_label = ttk.Label(root, text="Target Type:", style="Custom.TLabel", background="#ffffff")
target_label.grid(column=0, row=3, padx=5, pady=5)
target_menu = ttk.Combobox(root, textvariable=target_var)
target_menu['values'] = ["SQLSERVER", "SNOWFLAKE", "FILE"]
target_menu.grid(column=1, row=3, padx=5, pady=5)

target_frame = ttk.Frame(root, style="Custom.TFrame")
target_frame.grid(column=0, row=4, columnspan=2, padx=5, pady=5)

output_label = ttk.Label(root, text="Output Type:", style="Custom.TLabel", background="#ffffff")
output_label.grid(column=0, row=5, padx=5, pady=5)
output_menu = ttk.Combobox(root, textvariable=output_var)
output_menu['values'] = ["SQLSERVER", "SNOWFLAKE", "CSV"]
output_menu.grid(column=1, row=5, padx=5, pady=5)

output_frame = ttk.Frame(root, style="Custom.TFrame")
output_frame.grid(column=0, row=6, columnspan=2, padx=5, pady=5)

submit_button = ttk.Button(root, text="Submit", command=submit_form, style="Custom.TButton")
submit_button.grid(column=0, row=7, columnspan=2, pady=5)

# Create a style
style.configure("Custom.TFrame", background="#ffffff")

# Bind the update functions to the variables

source_var.trace('w', update_source_fields)
target_var.trace('w', update_target_fields)
output_var.trace('w', update_output_fields)


root.state('zoomed')

root.mainloop()
