FROM postgres:16.0

# Copy the custom entrypoint script
COPY entrypoint.sh /usr/local/bin/

# Copy the database initialization script
# PostgreSQL ejecuta automáticamente todos los scripts en /docker-entrypoint-initdb.d/
COPY init-databases.sh /docker-entrypoint-initdb.d/

# Make sure the scripts are executable
RUN chmod +x /usr/local/bin/entrypoint.sh && \
    chmod +x /docker-entrypoint-initdb.d/init-databases.sh

# Set the custom script as the entrypoint
ENTRYPOINT ["entrypoint.sh"]
