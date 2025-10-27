# Utiliser une image Python de base
FROM python:3.10-slim

# Définir le répertoire de travail dans le conteneur
WORKDIR /app

# Copier les dépendances et les installer
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le reste de l'application
COPY . .

# Exposer le port par défaut de Flask
EXPOSE 5000

# Définir la commande de démarrage
CMD ["python", "app.py"]