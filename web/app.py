import os
import redis
import psycopg2
from flask import Flask, jsonify, request, abort
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# --- Configuration et Connexions ---

# Configuration PostgreSQL
DB_HOST = os.getenv("POSTGRES_HOST", "db")
DB_NAME = os.getenv("POSTGRES_DB", "mydb")
DB_USER = os.getenv("POSTGRES_USER", "user")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "password")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

# Configuration Redis
REDIS_HOST = os.getenv("REDIS_HOST", "cache")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")

# Connexion aux bases de données
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"Erreur de connexion PostgreSQL: {e}")
        return None

try:
    # Test et initialisation de la connexion Redis (utilisé pour un "cache" simple ici)
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    redis_client.ping()
    print("Connexion Redis réussie")
except Exception as e:
    print(f"Erreur de connexion Redis: {e}")
    redis_client = None

# Création de la table utilisateur si elle n'existe pas
def init_db():
    conn = get_db_connection()
    if conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(80) UNIQUE NOT NULL,
                    email VARCHAR(120) UNIQUE NOT NULL
                );
            """)
            conn.commit()
            print("Table 'users' vérifiée/créée.")
        conn.close()

# Initialisation de la BDD au lancement
init_db()


# --- Health Check ---
@app.route('/health', methods=['GET'])
def health_check():
    # Vérification PostgreSQL
    db_status = 'OK'
    try:
        conn = get_db_connection()
        if not conn:
            db_status = 'ERROR - DB connection failed'
        else:
            conn.close()
    except:
        db_status = 'ERROR - DB exception'

    # Vérification Redis
    redis_status = 'OK'
    try:
        if not redis_client or not redis_client.ping():
             redis_status = 'ERROR - Redis connection failed'
    except:
        redis_status = 'ERROR - Redis exception'

    status_code = 200 if db_status == 'OK' and redis_status == 'OK' else 503

    return jsonify({
        "status": "UP",
        "database": db_status,
        "cache": redis_status
    }), status_code


# --- Endpoints CRUD Utilisateur ---

# POST /users : Créer un utilisateur
@app.route('/users', methods=['POST'])
def create_user():
    data = request.get_json()
    username = data.get('username')
    email = data.get('email')

    if not username or not email:
        return jsonify({"error": "Missing username or email"}), 400

    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database unavailable"}), 503

    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO users (username, email) VALUES (%s, %s) RETURNING id;", (username, email))
            user_id = cur.fetchone()[0]
            conn.commit()
            return jsonify({"id": user_id, "username": username, "email": email}), 201
    except psycopg2.IntegrityError:
        conn.rollback()
        return jsonify({"error": "Username or Email already exists"}), 409
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# GET /users : Lister les utilisateurs
@app.route('/users', methods=['GET'])
def list_users():
    # Exemple d'utilisation de Redis pour un "cache" simple
    if redis_client:
        cached_users = redis_client.get('all_users')
        if cached_users:
            print("Réponse depuis le cache Redis")
            return jsonify(eval(cached_users.decode('utf-8'))) # Simplifié pour la démo

    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database unavailable"}), 503

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, username, email FROM users;")
            users = [{"id": row[0], "username": row[1], "email": row[2]} for row in cur.fetchall()]

            # Mise en cache Redis
            if redis_client:
                redis_client.set('all_users', str(users), ex=30) # Cache de 30 secondes

            return jsonify(users)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# GET /users/<id> : Récupérer un utilisateur
@app.route('/users/<int:user_id>', methods=['GET'])
def get_user(user_id):
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database unavailable"}), 503

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, username, email FROM users WHERE id = %s;", (user_id,))
            user = cur.fetchone()
            if user:
                return jsonify({"id": user[0], "username": user[1], "email": user[2]})
            else:
                return jsonify({"error": "User not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# PUT /users/<id> : Modifier un utilisateur
@app.route('/users/<int:user_id>', methods=['PUT'])
def update_user(user_id):
    data = request.get_json()
    username = data.get('username')
    email = data.get('email')

    if not username and not email:
        return jsonify({"error": "No data provided for update"}), 400

    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database unavailable"}), 503

    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET username = %s, email = %s WHERE id = %s RETURNING id;", (username, email, user_id))
            if cur.rowcount == 0:
                conn.rollback()
                return jsonify({"error": "User not found"}), 404
            conn.commit()
            # Invalider le cache après modification
            if redis_client:
                 redis_client.delete('all_users')
            return jsonify({"id": user_id, "message": "User updated"})
    except psycopg2.IntegrityError:
        conn.rollback()
        return jsonify({"error": "Username or Email already exists"}), 409
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# DELETE /users/<id> : Supprimer un utilisateur
@app.route('/users/<int:user_id>', methods=['DELETE'])
def delete_user(user_id):
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database unavailable"}), 503

    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM users WHERE id = %s;", (user_id,))
            if cur.rowcount == 0:
                conn.rollback()
                return jsonify({"error": "User not found"}), 404
            conn.commit()
            # Invalider le cache après suppression
            if redis_client:
                 redis_client.delete('all_users')
            return jsonify({"id": user_id, "message": "User deleted"})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


if __name__ == '__main__':
    # Le port 5000 est utilisé dans Dockerfile et docker-compose
    app.run(host='0.0.0.0', port=5000)