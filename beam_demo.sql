CREATE DATABASE IF NOT EXISTS beam_demo;

USE beam_demo;

CREATE TABLE IF NOT EXISTS employees (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(150) NOT NULL,
    department VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);