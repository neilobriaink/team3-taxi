# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — RAG Workflow
# MAGIC **Purpose**: Retrieval-Augmented Generation demo using NYC taxi policy documents.
# MAGIC
# MAGIC **Approach**:
# MAGIC 1. Create a curated document corpus about NYC taxi operations
# MAGIC 2. Chunk documents into smaller passages
# MAGIC 3. Generate embeddings (tries Foundation Model API → sentence-transformers → TF-IDF)
# MAGIC 4. Store embeddings in a searchable Delta table
# MAGIC 5. Build a retrieval + generation pipeline for Q&A
# MAGIC
# MAGIC | Input | Output |
# MAGIC |-------|--------|
# MAGIC | Curated taxi documents | `rag_embeddings` Delta table + Q&A demo |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,Install mlflow dependencies
# MAGIC %pip install "mlflow>=2.10,<2.15" "pydantic<2.10" "typing_extensions>=4.12" -q

# COMMAND ----------

# DBTITLE 1,Fix typing_extensions + import mlflow
import sys

# Flush cached modules that conflict with the pip-installed versions
for _k in list(sys.modules.keys()):
    if any(x in _k for x in ['typing_extensions', 'pydantic', 'mlflow', 'opentelemetry', 'annotated_types']):
        del sys.modules[_k]

# Put pip-installed packages first on sys.path
_pip = [p for p in sys.path if '/local_disk0/' in p]
for p in _pip:
    sys.path.remove(p)
    sys.path.insert(0, p)

import mlflow
import mlflow.deployments
print(f"✓ mlflow {mlflow.__version__} ready")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Create Document Corpus
# MAGIC
# MAGIC These documents represent a curated knowledge base about NYC taxi operations.
# MAGIC In a production system, these would come from PDFs, wikis, APIs, or databases.

# COMMAND ----------

documents = [
    {
        "doc_id": "fare_structure",
        "title": "NYC Yellow Taxi Fare Structure",
        "content": (
            "NYC Yellow Taxi fares are regulated by the Taxi and Limousine Commission (TLC). "
            "The metered fare starts with an initial charge of $3.00 upon entry. "
            "The per-mile rate is $2.50 per mile when travelling above 12 mph. "
            "When the taxi is slow or stopped in traffic, the rate is $0.50 per minute. "
            "A $1.00 surcharge applies to peak hours (weekdays 4-8 PM). "
            "A $0.50 surcharge applies for overnight trips (8 PM to 6 AM). "
            "A $0.30 improvement surcharge is added to all trips. "
            "The MTA State Surcharge of $0.50 applies to all trips. "
            "Tolls are passed through to the passenger at cost. "
            "A congestion surcharge of $2.50 applies for trips that enter, leave, or pass through "
            "Manhattan south of 96th Street. "
            "Tips are not included in the metered fare and are at the discretion of the passenger."
        ),
    },
    {
        "doc_id": "flat_rates",
        "title": "Flat Rate Fares and Special Routes",
        "content": (
            "A flat fare of $70.00 (plus tolls and surcharges) applies between JFK Airport and Manhattan. "
            "This flat rate applies in both directions. "
            "For trips between JFK and locations outside Manhattan, the metered rate applies. "
            "Newark Airport trips are charged at the metered rate plus a $20.00 surcharge. "
            "LaGuardia Airport trips are always charged at the metered rate. "
            "Group rides (shared taxis) from JFK to Manhattan have a per-person flat rate. "
            "For trips originating at JFK, drivers must use the route requested by the passenger "
            "or the fastest route if no preference is stated."
        ),
    },
    {
        "doc_id": "payment_methods",
        "title": "Payment Methods and Policies",
        "content": (
            "All NYC Yellow Taxis are required to accept credit cards, debit cards, and cash. "
            "Drivers cannot refuse a fare based on the payment method chosen by the passenger. "
            "Credit card payments are processed through the Taxi Technology System installed in each vehicle. "
            "The payment system automatically suggests tip amounts of 20%, 25%, and 30% for credit card transactions. "
            "Passengers paying by credit card receive a printed receipt. "
            "Cash-paying passengers can request a receipt from the driver. "
            "Refusing a credit card payment is a TLC violation and can result in fines and license suspension. "
            "Contactless payment and mobile wallets (Apple Pay, Google Pay) are accepted on most newer payment terminals."
        ),
    },
    {
        "doc_id": "passenger_rights",
        "title": "Passenger Rights and Responsibilities",
        "content": (
            "Passengers have the right to direct the route of their trip. "
            "Passengers may choose to ride in silence (drivers must comply). "
            "The driver must turn on the air conditioning or heat upon request. "
            "Passengers can request the driver to stop playing the radio or reduce the volume. "
            "Passengers are entitled to a safe, clean vehicle and a courteous driver. "
            "Service animals must be accommodated at all times. "
            "Passengers should not be charged more than the metered fare (except for tolls and surcharges). "
            "Complaints can be filed via 311, the TLC website, or the TLC Passenger App. "
            "Passengers can request the driver's TLC license number at any time. "
            "All taxis must have a functioning partition between the driver and passenger compartment."
        ),
    },
    {
        "doc_id": "driver_requirements",
        "title": "Driver Requirements and Licensing",
        "content": (
            "NYC Yellow Taxi drivers must hold a valid TLC-issued Hack License (officially called a Taxi Driver License). "
            "To obtain a license, drivers must be at least 19 years old with a valid US driver's license for at least one year. "
            "Drivers must complete a TLC-approved driver education course (24 hours). "
            "A defensive driving course is required before the license is issued. "
            "Drivers must pass a drug test and a background check including fingerprinting. "
            "The TLC license must be renewed every three years. "
            "Drivers must display their TLC license prominently in the vehicle at all times. "
            "Maximum shift length is 12 hours. Drivers must not be on the road for more than 12 hours in any 24-hour period."
        ),
    },
    {
        "doc_id": "zones_boroughs",
        "title": "NYC Taxi Zones and Borough Coverage",
        "content": (
            "NYC is divided into 263 taxi zones across five boroughs for tracking and analytics purposes. "
            "Manhattan has the highest concentration of taxi pickups, particularly Midtown and the Financial District. "
            "Yellow Taxis can pick up street-hail passengers anywhere in NYC but primarily operate in Manhattan and at airports. "
            "In the outer boroughs (Brooklyn, Queens, Bronx, Staten Island), green Boro Taxis provide dedicated service. "
            "Key high-traffic zones include: Penn Station/Madison Square Garden (Zone 186), Times Square (Zone 161/162), "
            "Grand Central (Zone 90), Upper East Side (Zone 236/237), and the three major airports: "
            "JFK (Zone 132), LaGuardia (Zone 138), and Newark (tracked separately in NJ). "
            "The congestion pricing zone covers Manhattan south of 96th Street."
        ),
    },
    {
        "doc_id": "vehicle_requirements",
        "title": "Vehicle Standards and Requirements",
        "content": (
            "All NYC Yellow Taxis must be painted the official Dupont M6284 yellow colour. "
            "Vehicles must pass an annual TLC inspection covering safety, cleanliness, and equipment. "
            "Taxis must be equipped with a functioning taximeter, a payment terminal, and a Taxi Technology System. "
            "The rooftop medallion light must be operational: lit when available, off when occupied, and half-lit when off-duty. "
            "Vehicles must have working air conditioning, heating, and seat belts for all passengers. "
            "Maximum vehicle age is typically 5-7 years depending on the vehicle type. "
            "Wheelchair-accessible vehicles (WAVs) make up 50% of the fleet as mandated by the TLC. "
            "All taxis must display rate decals on the rear passenger doors showing the fare structure. "
            "Electric and hybrid vehicles are increasingly common and receive incentives from the TLC."
        ),
    },
    {
        "doc_id": "tipping_guide",
        "title": "Tipping Guide for NYC Taxi Rides",
        "content": (
            "Tipping is customary but not mandatory for NYC taxi rides. "
            "The standard tip ranges from 15% to 20% of the metered fare. "
            "For exceptional service, passengers may tip 25% or more. "
            "Credit card payment terminals suggest tip options of 20%, 25%, and 30%. "
            "Cash tips should be given directly to the driver. "
            "Tips are not included in the metered fare or any surcharges. "
            "Drivers rely on tips as a significant portion of their income. "
            "For flat-rate JFK trips ($70), a tip of $10-15 is typical. "
            "Drivers who assist with luggage generally receive slightly higher tips. "
            "During holidays and severe weather, higher tips (20-25%) are considered appropriate as a courtesy."
        ),
    },
    {
        "doc_id": "lost_property",
        "title": "Lost Property and Found Items Policy",
        "content": (
            "If you leave an item in a NYC Yellow Taxi, contact 311 or visit the TLC website to file a lost property report. "
            "You will need the trip details: approximate time, pickup/dropoff locations, and ideally the medallion number or receipt. "
            "The TLC maintains a lost property database that matches reports with items turned in by drivers. "
            "Drivers are required to check the vehicle after each trip and turn in found items. "
            "Items are held at the NYC Taxi Lost Property office at the TLC headquarters in Long Island City, Queens. "
            "Commonly lost items include phones, wallets, umbrellas, and bags. "
            "The TLC Passenger App can help identify the specific taxi you rode in using GPS records. "
            "Filing a report promptly increases the chances of recovery."
        ),
    },
    {
        "doc_id": "safety_regulations",
        "title": "Safety Regulations and Emergency Procedures",
        "content": (
            "NYC Yellow Taxis are subject to strict safety oversight by the TLC and the NYPD. "
            "All vehicles undergo semi-annual safety inspections covering brakes, tyres, lights, and structural integrity. "
            "Drivers must not use mobile phones while driving (hands-free is allowed). "
            "Speed limiters are not mandatory, but reckless driving will result in TLC penalties. "
            "In case of an accident, the driver must exchange information and file a TLC accident report within 24 hours. "
            "Passengers injured in a taxi accident should note the medallion number and driver info. "
            "Emergency exits: passengers can exit from either rear door. Partitions must have an emergency opening mechanism. "
            "All taxis carry a basic first aid kit as part of TLC equipment requirements. "
            "If a passenger feels unsafe, they can ask the driver to stop and exit the vehicle, or call 911."
        ),
    },
]

print(f"Document corpus: {len(documents)} documents")
for doc in documents:
    print(f"  - {doc['title']} ({len(doc['content'])} chars)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Chunk Documents
# MAGIC
# MAGIC Split each document into smaller overlapping chunks for more precise retrieval.

# COMMAND ----------

def chunk_text(text, chunk_size=150, overlap=20):
    """Split text into overlapping chunks by word count."""
    words = text.split()
    chunks = []
    start = 0
    while start < len(words):
        end = start + chunk_size
        chunk = " ".join(words[start:end])
        chunks.append(chunk.strip())
        start = end - overlap
    return chunks

all_chunks = []
for doc in documents:
    chunks = chunk_text(doc["content"], chunk_size=150, overlap=20)
    for i, chunk in enumerate(chunks):
        all_chunks.append({
            "doc_id": doc["doc_id"],
            "title": doc["title"],
            "chunk_id": f"{doc['doc_id']}_chunk_{i}",
            "chunk_text": chunk,
        })

print(f"Total chunks: {len(all_chunks)}")
for c in all_chunks[:3]:
    print(f"  [{c['chunk_id']}] {c['chunk_text'][:80]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Generate Embeddings
# MAGIC
# MAGIC Tries three approaches in order:
# MAGIC 1. **Databricks Foundation Model API** (`databricks-gte-large-en`) — best quality
# MAGIC 2. **sentence-transformers** (`all-MiniLM-L6-v2`) — good local alternative
# MAGIC 3. **TF-IDF** — always-available fallback

# COMMAND ----------

embedding_method = None
embedding_model_obj = None
tfidf_vectoriser = None

# Attempt 1: Databricks Foundation Model API via mlflow.deployments
try:
    import mlflow.deployments
    client = mlflow.deployments.get_deploy_client("databricks")
    test_response = client.predict(
        endpoint="databricks-gte-large-en",
        inputs={"input": ["test embedding"]}
    )
    embedding_method = "databricks_fm"
    print("✓ Using Databricks Foundation Model API (databricks-gte-large-en)")
except Exception as e:
    print(f"Databricks FM API not available: {e}")

# COMMAND ----------

# Attempt 2: sentence-transformers
if embedding_method is None:
    try:
        from sentence_transformers import SentenceTransformer
        embedding_model_obj = SentenceTransformer("all-MiniLM-L6-v2")
        embedding_method = "sentence_transformers"
        print("✓ Using sentence-transformers (all-MiniLM-L6-v2)")
    except ImportError:
        print("sentence-transformers not installed")

# COMMAND ----------

# Attempt 3: TF-IDF fallback (always works)
if embedding_method is None:
    from sklearn.feature_extraction.text import TfidfVectorizer
    embedding_method = "tfidf"
    print("✓ Using TF-IDF embeddings (fallback — always available)")

print(f"\nEmbedding method: {embedding_method}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Embeddings

# COMMAND ----------

def embed_texts(texts):
    """Embed a list of texts using the selected method."""
    if embedding_method == "databricks_fm":
        response = client.predict(
            endpoint="databricks-gte-large-en",
            inputs={"input": texts}
        )
        return [item["embedding"] for item in response["data"]]

    elif embedding_method == "sentence_transformers":
        embeddings = embedding_model_obj.encode(texts, show_progress_bar=True)
        return [emb.tolist() for emb in embeddings]

    elif embedding_method == "tfidf":
        global tfidf_vectoriser
        tfidf_vectoriser = TfidfVectorizer(max_features=256, stop_words="english")
        tfidf_matrix = tfidf_vectoriser.fit_transform(texts)
        return [tfidf_matrix[i].toarray().flatten().tolist() for i in range(len(texts))]

# COMMAND ----------

texts = [c["chunk_text"] for c in all_chunks]
embeddings = embed_texts(texts)

pdf = pd.DataFrame(all_chunks)
pdf["embedding"] = embeddings

print(f"✓ Generated embeddings for {len(pdf)} chunks")
print(f"  Embedding dimension: {len(pdf['embedding'].iloc[0])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Store Embeddings in Delta Table

# COMMAND ----------

# Serialise embeddings as JSON strings for Delta storage
pdf["embedding_str"] = pdf["embedding"].apply(json.dumps)

schema = StructType([
    StructField("doc_id", StringType(), False),
    StructField("title", StringType(), False),
    StructField("chunk_id", StringType(), False),
    StructField("chunk_text", StringType(), False),
    StructField("embedding_str", StringType(), False),
])

df_embeddings = spark.createDataFrame(
    pdf[["doc_id", "title", "chunk_id", "chunk_text", "embedding_str"]],
    schema=schema,
)

(
    df_embeddings
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(RAG_EMBEDDINGS)
)

print(f"✓ Embeddings stored: {RAG_EMBEDDINGS} ({df_embeddings.count()} chunks)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Build Retrieval Function

# COMMAND ----------

def cosine_similarity(a, b):
    """Compute cosine similarity between two vectors."""
    a = np.array(a, dtype=np.float64)
    b = np.array(b, dtype=np.float64)
    dot = np.dot(a, b)
    norm = np.linalg.norm(a) * np.linalg.norm(b)
    if norm == 0:
        return 0.0
    return float(dot / norm)


def embed_query(query_text):
    """Embed a single query using the active embedding method."""
    if embedding_method == "databricks_fm":
        response = client.predict(
            endpoint="databricks-gte-large-en",
            inputs={"input": [query_text]}
        )
        return response["data"][0]["embedding"]

    elif embedding_method == "sentence_transformers":
        return embedding_model_obj.encode([query_text])[0].tolist()

    elif embedding_method == "tfidf":
        return tfidf_vectoriser.transform([query_text]).toarray().flatten().tolist()


def retrieve(query, top_k=3):
    """Retrieve the most relevant chunks for a query."""
    query_emb = embed_query(query)

    # Compute similarity against all stored chunks
    similarities = []
    for _, row in pdf.iterrows():
        sim = cosine_similarity(query_emb, row["embedding"])
        similarities.append({
            "title": row["title"],
            "chunk_text": row["chunk_text"],
            "similarity": sim,
        })

    # Sort and return top-k
    similarities.sort(key=lambda x: x["similarity"], reverse=True)
    return similarities[:top_k]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Build RAG Q&A Pipeline

# COMMAND ----------

def build_rag_prompt(query, context_chunks):
    """Build a prompt combining retrieved context with the user query."""
    context = "\n\n".join([
        f"[Source: {c['title']}]\n{c['chunk_text']}"
        for c in context_chunks
    ])

    return f"""You are a veteran New York City yellow cab driver with 20 years on the streets of Manhattan. You've seen it all — tourists, regulars, rush hour gridlock, airport runs at 4 AM. Answer the passenger's question in your authentic NYC cabbie voice: friendly, straight-talking, maybe a little opinionated, and full of local flavour. Throw in the occasional "buddy", "pal", or "listen" — but keep the facts accurate. Base your answer ONLY on the provided context. If the context doesn't cover it, say something like "Hey, that's above my pay grade" and suggest where they might find out.

CONTEXT:
{context}

PASSENGER'S QUESTION: {query}

YOUR ANSWER:"""


def rag_query(query, top_k=3):
    """Full RAG pipeline: retrieve relevant context, augment prompt, generate answer."""
    print(f"Query: {query}")
    print("-" * 60)

    # Step 1: Retrieve
    results = retrieve(query, top_k=top_k)
    print(f"\nRetrieved {len(results)} relevant chunks:")
    for i, r in enumerate(results):
        print(f"  {i+1}. [{r['title']}] (similarity: {r['similarity']:.4f})")
        print(f"     {r['chunk_text'][:100]}...")

    # Step 2: Build prompt
    prompt = build_rag_prompt(query, results)

    # Step 3: Generate answer (try LLM, fall back to context display)
    answer = None
    try:
        import mlflow.deployments
        gen_client = mlflow.deployments.get_deploy_client("databricks")
        response = gen_client.predict(
            endpoint="databricks-meta-llama-3-3-70b-instruct",
            inputs={
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 500,
            }
        )
        answer = response["choices"][0]["message"]["content"]
    except Exception as e:
        print(f"\n(LLM generation unavailable: {e})")

    if answer is None:
        # Fallback: present retrieved context directly
        answer = "**Retrieved context** (LLM not available for generation):\n\n"
        for r in results:
            answer += f"- **{r['title']}**: {r['chunk_text']}\n\n"

    print(f"\nAnswer:\n{answer}")
    print("=" * 60)
    return {"query": query, "answer": answer, "sources": results}

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Demo: Ask Questions About NYC Taxis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q1: What is the fare from JFK to Manhattan?

# COMMAND ----------

result1 = rag_query("What is the fare from JFK Airport to Manhattan?")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q2: How should I tip my taxi driver?

# COMMAND ----------

result2 = rag_query("How much should I tip my NYC taxi driver?")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q3: What payment methods are accepted?

# COMMAND ----------

result3 = rag_query("Can I pay with a credit card in a NYC yellow taxi?")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q4: What do I do if I lose something?

# COMMAND ----------

result4 = rag_query("I left my phone in a taxi. What should I do?")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q5: Peak hours and surcharges?

# COMMAND ----------

result5 = rag_query("When are peak hour surcharges applied and how much are they?")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## RAG Architecture Summary
# MAGIC
# MAGIC ### Pipeline
# MAGIC ```
# MAGIC Documents → Chunking → Embedding → Delta Table (Vector Store)
# MAGIC                                          ↓
# MAGIC User Query → Query Embedding → Cosine Similarity Search → Top-K Chunks
# MAGIC                                                                ↓
# MAGIC                                                    LLM Prompt (Context + Query) → Answer
# MAGIC ```
# MAGIC
# MAGIC ### Components
# MAGIC | Component | Implementation |
# MAGIC |-----------|---------------|
# MAGIC | Corpus | 10 curated NYC taxi policy documents |
# MAGIC | Chunking | ~150 word overlapping chunks |
# MAGIC | Embeddings | Best available: FM API → sentence-transformers → TF-IDF |
# MAGIC | Vector Store | Delta table with serialised embeddings |
# MAGIC | Retrieval | Cosine similarity, top-3 |
# MAGIC | Generation | Databricks FM API (Llama 3.1 70B), with context-only fallback |
# MAGIC
# MAGIC ### Strengths
# MAGIC - Answers are **grounded in real documents**, reducing hallucination risk
# MAGIC - Retrieval scores provide **transparency** on source relevance
# MAGIC - **Modular design**: embedding and generation components can be swapped independently
# MAGIC - Works with or without LLM access (graceful degradation)
# MAGIC
# MAGIC ### Limitations & Future Improvements
# MAGIC - Small corpus (10 documents) — production systems would use thousands
# MAGIC - No reranking step between retrieval and generation
# MAGIC - No evaluation framework for answer quality (→ Stretch Goal D)
# MAGIC - No safety/guardrail filtering on queries or outputs (→ Stretch Goal D)
