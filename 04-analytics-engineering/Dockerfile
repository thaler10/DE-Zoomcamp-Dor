# משתמשים באימג' של גוגל שכולל כבר את ה-SDK
FROM google/cloud-sdk:latest

# התקנת דרישות קדם, הוספת המפתח של Hashicorp והתקנת Terraform
RUN apt-get update && apt-get install -y gnupg software-properties-common wget \
    && wget -O- https://apt.releases.hashicorp.com/gpg | gpg --dearmor | tee /usr/share/keyrings/hashicorp-archive-keyring.gpg > /dev/null \
    && echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | tee /etc/apt/sources.list.d/hashicorp.list \
    && apt-get update && apt-get install -y terraform python3-pip

# מגדירים את תיקיית העבודה
WORKDIR /app

# מעתיקים את ה-requirements (אם יצרת קובץ כזה, אפילו ריק)
COPY requirements.txt .
# התקנת dbt-bigquery ישירות בבנייה
RUN pip3 install dbt-bigquery --break-system-packages
# פקודה שמשאירה את הקונטיינר פעיל
CMD ["sleep", "infinity"]