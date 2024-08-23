# Data Storage and Api Using Python 📁

## Overview 🔎
This project consists of two Python applications designed to work with MongoDB Atlas:

Data Storage App: A Python application that reads data saved JSON files and stores it into a MongoDB Atlas database.
API Service: A Flask application that provides API endpoints to interact with the data stored in MongoDB Atlas.

## Features ✨
### Data Storage App
JSON Data Ingestion: Efficiently reads and processes data stored in JSON files.
MongoDB Atlas Integration: Automatically stores structured data into a MongoDB Atlas database for scalable and secure storage.

### API Service 
Flask Framework: Lightweight and flexible module within python, perfect for building complex web applications.
CRUD Operations: Endpoints to Create, Read, Update, and Delete (CRUD) data within the MongoDB Atlas database.
Data Querying: Allows for complex queries on stored data to retrieve relevant information quickly..

## Setup Instructions 📝
### Prerequisites
pyhton 3.12 version
mongodb application, and account
Libraries pymongo, flask

### Running the Applications ▶
1st: Run the data_storage app to store the saved jsaon file in mongodb
2nd: After data stored, run the flask app to connect with mongodb and manipulate data

## Example API Endpoints
GET /top_keywords:
[
    {"_id": "غزة", "count": 10},
    {"_id": "إيران", "count": 8},
    ...
]
GET /top_authors:
[
    {"_id": "الميادين نت", "count": 25},
    {"_id": "Author Name", "count": 15},
    ...
]
