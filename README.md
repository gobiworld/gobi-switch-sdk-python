# Exécuter un ordre

Ce guide explique comment exécuter un ordre de transaction. Deux modes d'exécution sont disponibles :

1. **Exécution directe du script Python** `main.py`
2. **Exécution via Docker** à partir du `Dockerfile`
3. **Version de python recommandée**`3.11`

---

## Architecture du point d'entrée

Le fichier `main.py` constitue le point d'entrée de l'application. Il contient les composants nécessaires pour consommer un événement Kafka et exécuter le traitement associé.

### `GobiKafkaEvent`

`GobiKafkaEvent` représente l'événement Kafka reçu par l'application.

Il contient notamment les informations nécessaires pour identifier et traiter la **transaction à effectuer**.

### `BaseMessageHandler`

`BaseMessageHandler` définit le point d'extension permettant d'implémenter le traitement métier du payout.

Par défaut, le contenu de l'événement reçu est simplement affiché (`print`). Pour implémenter le traitement réel, il faut créer une classe qui **hérite de `BaseMessageHandler`** et implémente le comportement attendu.

Exemple :

```python
class PayoutMessageHandler(BaseMessageHandler):
    def handle(self, event: GobiKafkaEvent):
        # Implémenter ici le traitement du payout
        pass
```

### `KafkaConsumerGobiSDK`

`KafkaConsumerGobiSDK` constitue le cœur de l'application.

Il est responsable de :

* se connecter au cluster Kafka ;
* consommer les événements du topic configuré ;
* transmettre chaque événement au `BaseMessageHandler` ;
* exécuter la méthode `handle()` du handler implémenté.

Le flux d'exécution est donc :

```text
Kafka
  │
  ▼
KafkaConsumerGobiSDK
  │
  ▼
GobiKafkaEvent
  │
  ▼
BaseMessageHandler.handle()
  │
  ▼
Traitement du payout
```

---

# &. Exécuter `main.py` localement

## 2.1 Prérequis

Avant de lancer l'application, assurez-vous d'avoir :

* Python installé ;
* les dépendances du projet installées ;
* accès au cluster Kafka fourni par Gobi ;
* les credentials Kafka nécessaires ;
* les credentials de l'API Switch si l'enrichissement de la transaction est nécessaire.

---

## 1.2 Configurer les variables d'environnement

L'application utilise des variables d'environnement pour configurer sa connexion à Kafka.

Créez un fichier `.env` à la racine du projet ou exportez directement les variables dans votre environnement.

### Configuration Kafka

Les valeurs suivantes doivent être fournies par Gobi :

```dotenv
# Kafka core
KAFKA_TOPIC=
KAFKA_BOOTSTRAP_SERVERS=
KAFKA_GROUP_ID=

# Security & Access
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=SCRAM-SHA-512
KAFKA_SASL_USERNAME=
KAFKA_SASL_PASSWORD=
```

### Variables Kafka

| Variable                  | Description                       |
| ------------------------- | --------------------------------- |
| `KAFKA_TOPIC`             | Topic Kafka à consommer           |
| `KAFKA_BOOTSTRAP_SERVERS` | Adresse(s) du cluster Kafka       |
| `KAFKA_GROUP_ID`          | Identifiant du consumer group     |
| `KAFKA_SECURITY_PROTOCOL` | Protocole de sécurité Kafka       |
| `KAFKA_SASL_MECHANISM`    | Mécanisme d'authentification SASL |
| `KAFKA_SASL_USERNAME`     | Identifiant Kafka                 |
| `KAFKA_SASL_PASSWORD`     | Mot de passe Kafka                |

> **Attention :** ne committez jamais les credentials Kafka dans le repository.

---

## 1.3 Configurer l'API Switch

Le traitement nécessite de récupérer les détails d'une transaction afin d'enrichir le `GobiKafkaEvent`, configurez également les variables de l'API Switch pour par votre administrateur :
```dotenv
# API
SWITCH_API_HOST=https://api-switch-dev.gobiworld.com
SWITCH_API_PUBLIC_KEY=
SWITCH_API_PRIVATE_KEY=
SWITCH_API_MODE=sandbox
```

Ces variables permettent notamment de définir :

* l'URL de l'API ;
* la clé publique utilisée pour l'authentification ;
* la clé privée utilisée pour l'authentification ;
* l'environnement d'exécution (`sandbox`, `live`.).

---

## 1.4 Implémenter le traitement métier

Par défaut, `BaseMessageHandler` affiche le contenu de l'événement reçu.

Pour exécuter réellement le payout, créez votre propre handler en héritant de `BaseMessageHandler`.

Exemple conceptuel :

```python
class PayoutMessageHandler(BaseMessageHandler):

    def handle(self, message: GobiKafkaEvent):
        transaction = self.get_transaction_details(event)

        # Récupérer les détails de l'événement
        orderUid=message.value.transactionUuid,
        to_amount=message.value.amount,
        to_pm_identifier=message.transaction.service.slug,
        arguments=message.transaction.arguments,

        # Traitement du payout
        # execute_paiement(event)
```

L'implémentation exacte dépend du traitement métier attendu par votre application.

---

## 1.5 Lancer l'application

Une fois les variables d'environnement configurées et le handler implémenté, lancez le point d'entrée :

```bash
python main.py
```

L'application va alors :

1. initialiser le `KafkaConsumerGobiSDK` ;
2. se connecter à Kafka ;
3. écouter le topic configuré dans `KAFKA_TOPIC` ;
4. recevoir les `GobiKafkaEvent` ;
5. appeler le `handle()` du handler ;
6. exécuter le traitement du payout.

---

# 2. Exécuter l'application avec Docker

Le projet fournit également un `Dockerfile` permettant de construire une image Docker contenant l'application.

Cette approche permet d'exécuter l'application dans un environnement isolé et reproductible.

## 2.1 Construire l'image Docker

Depuis la racine du projet :

```bash
docker build -t gobi-payout .
```

Cette commande utilise le `Dockerfile` présent dans le projet pour construire l'image.

---

## 2.2 Configurer le fichier d'entrée

Le `Dockerfile` permet de définir le fichier Python à exécuter grâce à la variable d'environnement :

```dotenv
APP_MAIN=main.py
```

Par défaut, vous pouvez donc utiliser :

```dotenv
APP_MAIN=main.py
```

Si le projet contient plusieurs points d'entrée, cette variable permet de sélectionner celui qui doit être exécuté dans le conteneur.

---

## 2.3 Lancer le conteneur

Les variables d'environnement nécessaires à l'application doivent être transmises au conteneur.

Par exemple :

```bash
docker run --rm \
  -e KAFKA_TOPIC="$KAFKA_TOPIC" \
  -e KAFKA_BOOTSTRAP_SERVERS="$KAFKA_BOOTSTRAP_SERVERS" \
  -e KAFKA_GROUP_ID="$KAFKA_GROUP_ID" \
  -e KAFKA_SECURITY_PROTOCOL="$KAFKA_SECURITY_PROTOCOL" \
  -e KAFKA_SASL_MECHANISM="$KAFKA_SASL_MECHANISM" \
  -e KAFKA_SASL_USERNAME="$KAFKA_SASL_USERNAME" \
  -e KAFKA_SASL_PASSWORD="$KAFKA_SASL_PASSWORD" \
  -e SWITCH_API_HOST="$SWITCH_API_HOST" \
  -e SWITCH_API_PUBLIC_KEY="$SWITCH_API_PUBLIC_KEY" \
  -e SWITCH_API_PRIVATE_KEY="$SWITCH_API_PRIVATE_KEY" \
  -e SWITCH_API_MODE="$SWITCH_API_MODE" \
  -e APP_MAIN="main.py" \
  gobi-payout
```

> **Conseil :** pour éviter de passer manuellement chaque variable avec `-e`, vous pouvez utiliser un fichier d'environnement avec `--env-file`.

Par exemple :

```bash
docker run --rm --env-file .env gobi-payout
```

---

# 4. Résumé du fonctionnement

Le traitement complet peut être résumé ainsi :

```text
                     ┌──────────────────────┐
                     │        Kafka         │
                     └──────────┬───────────┘
                                │
                                │ Event
                                ▼
                     ┌──────────────────────┐
                     │ KafkaConsumerGobiSDK │
                     └──────────┬───────────┘
                                │
                                │ GobiKafkaEvent
                                ▼
                     ┌──────────────────────┐
                     │  BaseMessageHandler  │
                     └──────────┬───────────┘
                                │
                                │ handle()
                                ▼
                   ┌────────────────────────────┐
                   │ Traitement à votre niveau  │
                   └────────────────────────────┘
```

L'application peut être exécutée de deux manières :

| Mode       | Commande / mécanisme             | Usage                                     |
| ---------- | -------------------------------- | ----------------------------------------- |
| **Python** | `python main.py`                 | Développement et debug local              |
| **Docker** | `docker build` puis `docker run` | Environnement reproductible / déploiement |

Le traitement métier doit être implémenté dans une classe héritant de `BaseMessageHandler`. `KafkaConsumerGobiSDK` se charge ensuite de la consommation Kafka et de l'appel au handler.

# Important 
Il n'est pas nécessaire d'appeler Switch après le traitement du handle pour déclencher le processus de compensation.

Dès lors que l'exécution de `handle()` se termine sans exception, KafkaConsumerGobiSDK appelle automatiquement l'API payout_callback(`/api/paiement/transactions/payout-callback/`), qui déclenche le processus de compensation.