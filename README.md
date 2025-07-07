# Nasdaq Stock Screener

Application de filtrage d'actions sur le NASDAQ selon 142 conditions horaires personnalisées à partir de données fournies par Interactive Brokers.

## 🗂 Structure du projet

```
project/
├── app.py
├── main.py
├── config.py
├── conditions.py
├── data_handler.py
├── gui_handler.py
├── utils.py
├── output/
│   └── screener_results.txt
└── README.md
```

## ⚙️ Dépendances

- Python 3.9+
- ib_insync
- pandas
- pytz
- tkinter (intégré à Python)

Installation :

```bash
pip install ib_insync pandas pytz
```

## 🚀 Lancer l'application

Lancez l'application Trader Workstation en mode administrateur en version démo (ou connectez vous si vous avez un compte).
Fichier > Configuration Générale > API > Settings : - Cochez 'Enable ActiveX and Socket Clients
                                                    - Décochez 'Read-Only API'

Puis lancez :

```bash
python main.py
```

Cela ouvre l'application en plein écran avec l'interface graphique.

## 📥 Import de tickers

Le fichier doit contenir une liste de tickers séparés par des virgules, par exemple :

```
AAPL,MSFT,GOOGL,TSLA
```

**Maximum : 50 tickers.**

## 🧠 Fonctionnement

- Connexion à Interactive Brokers via `ib_insync`
- Téléchargement des données horaires (sur 7 jours, en heures étendues)
- Application des conditions sélectionnées
- Résultats affichés dans l’interface + export dans `output/screener_results.txt`

## 📝 Fichiers principaux

| Fichier          | Rôle                                                         |
|------------------|--------------------------------------------------------------|
| `main.py`        | Lancement de l'application (fullscreen, gestion interface)   |
| `app.py`         | Contient la logique métier de lancement                     |
| `config.py`      | Configuration globale (logs, connexion IB, constantes)       |
| `conditions.py`  | Définition structurée des 142 conditions techniques          |
| `data_handler.py`| Téléchargement et traitement des données de marché           |
| `gui_handler.py` | Création de l’interface Tkinter                              |
| `utils.py`       | Fonctions utilitaires (comparateurs, extractions, conversions) |

## 💾 Résultats

Les résultats sont enregistrés automatiquement dans :

```
output/screener_results.txt
```

Format :

```
Serial    TickerNo    Ticker    Open16hDay-1
```

## 🧩 Personnalisation

- Icône : `money_analyze_icon_143358.ico` si disponible.
- Fuseau horaire : US/Eastern.
- Données utilisées : barres horaires sur les 7 derniers jours.

## 📄 Licence

Projet open-source. Usage personnel ou professionnel autorisé.
