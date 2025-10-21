FROM ibmcom/db2:latest

# Copy initialization scripts
COPY init-scripts/ /var/custom/

# Copy entrypoint script
COPY docker-entrypoint.sh /var/custom/docker-entrypoint.sh

RUN chmod +x /var/custom/docker-entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/var/custom/docker-entrypoint.sh"]
