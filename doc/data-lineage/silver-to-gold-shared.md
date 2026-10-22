# Data Lineage – Silver to Gold (Shared)

Generated: 2026-02-20 15:40:17 UTC

## Übersicht

| Notebook | Silver Sources | Gold Target |
|---|---|---|
| [03_NB_WriteIntoDimDate](fabric/03_Silver2Gold/Shared/03_NB_WriteIntoDimDate.Notebook/notebook-content.py) | (keine Silver-Quelle erkannt) | LH_Gold.Shared.DimDate |
| [03_NB_WriteIntoDimTimeOfDay](fabric/03_Silver2Gold/Shared/03_NB_WriteIntoDimTimeOfDay.Notebook/notebook-content.py) | (keine Silver-Quelle erkannt) | LH_Gold.Shared.DimTimeOfDay |

## Lineage Graph

::: mermaid
flowchart LR
    LH_Gold_Shared_DimDate["LH_Gold.Shared.DimDate"]
    LH_Gold_Shared_DimTimeOfDay["LH_Gold.Shared.DimTimeOfDay"]
:::

## Column-Level Lineage

Die folgenden Mappings werden statisch aus den `df_transformed.select(...)` Ausdrücken der Notebooks abgeleitet.

### LH_Gold.Shared.DimDate

Notebook: [03_NB_WriteIntoDimDate](fabric/03_Silver2Gold/Shared/03_NB_WriteIntoDimDate.Notebook/notebook-content.py)

(Keine Spalten-Mappings erkannt)

### LH_Gold.Shared.DimTimeOfDay

Notebook: [03_NB_WriteIntoDimTimeOfDay](fabric/03_Silver2Gold/Shared/03_NB_WriteIntoDimTimeOfDay.Notebook/notebook-content.py)

(Keine Spalten-Mappings erkannt)
