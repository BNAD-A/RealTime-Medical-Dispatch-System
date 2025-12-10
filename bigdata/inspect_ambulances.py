# inspect_ambulances.py
import pandas as pd

AMB = r"C:\Users\pc\Projects\Urgences\bigdata\data\ambulances_clean.csv"
df = pd.read_csv(AMB)
print("Total ambulances:", len(df))
print("Status counts:\n", df['statut'].value_counts(dropna=False))
print("\nDisponibles par ville (top 30):")
print(df[df['statut']=='disponible'].groupby('ville').size().sort_values(ascending=False).head(30))
print("\nTotal disponibles:", len(df[df['statut']=='disponible']))
