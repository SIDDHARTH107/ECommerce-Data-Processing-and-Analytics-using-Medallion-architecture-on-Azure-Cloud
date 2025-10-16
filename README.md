# Real Time ECommerce Data Processing & Analytics using Medallion Architecture on Azure Cloud

## Architecture
<img width="1219" height="693" alt="image" src="https://github.com/user-attachments/assets/2b3540df-6098-42b0-8d68-65d9795aaf54" />

## Medallion Architecture
![Alt text](https://github.com/SIDDHARTH107/Real-Time-E-Commerce-Data-Processing-and-Analytics-using-Medallion-architecture-on-Azure-Cloud/blob/main/1719345910378.gif?raw=true)

<img width="1355" height="727" alt="image" src="https://github.com/user-attachments/assets/73f99e14-da68-4a71-8c3c-6c1d78c74c90" />

**Microsoft Azure** gives us 2 modes: Pay as you go and trying for free. But, if we are choosing free, it will give us the free $200 credits to use it for 1 month. 
<img width="1890" height="730" alt="image" src="https://github.com/user-attachments/assets/9574d94b-e921-45da-a467-3e3adedb0f19" />

# Challenges Faced
1. Built a separate pipeline for olist_geolocation from PostgreSQL to Azure bronze layer (ADLS Gen2) because of the timeout issue due to the large volume of data (around 10 lakhs records).
2. Used Self Hosted Integrated Runtime (SHIR), which acts as a gateway from the Local PostgreSQL server to the Azure cloud, as local to cloud is not possible without any gateway.
3. Used the Publish All button for permanent save otherwise I would have taken around 2-3 weeks more to start building the pipeline from scratch, which took me a lot of time.
4. To make the connection, we need some key or permission here to maintain the equality. I mean, both are Microsoft Azure services, so they are connected within Microsoft, and no third party can access them. So, here in this case when I tried to connect Azure Databricks and ADLS, I got this error:

<img width="839" height="376" alt="image" src="https://github.com/user-attachments/assets/bb8595c4-ed56-401a-b750-331bfe4e25d4" />

What This Error Means
The error "You do not have access" with Error code: 401 means "Unauthorized." It's a permissions issue. I was successfully logged into Azure, but my specific account (mohepatra.si@northeastern.edu) does not have the administrative rights to view or manage the "App registrations" section.

Why I didn’t Have Access (The University Analogy)
Think of your university's entire Microsoft Azure setup as a large office building.

The Building (Azure Active Directory/Tenant): The entire building is managed by your university's IT administrators. They control the main entrance, the security office, and who gets what keys. The "App registrations" page is like the building's main security office. It's a central, high-level area.

Your Office (Your Azure for Students Subscription): The university has given you your own private office inside the building—this is your "Azure for Students" subscription. Inside your office, you are the owner. You can set up furniture (like Azure Data Factory, Databricks, etc.) and do whatever you want.

The error we are seeing is because our keycard (our student account) lets us into our own office, but it doesn't grant us access to the main security office for the entire building. This is a security measure to prevent students from viewing or changing administrative settings that affect the whole university.

However, the tutorial I was following demonstrated the most common and robust method for production environments: creating a Service Principal. A Service Principal is like creating a robot user with its own specific permissions. However, as you discovered, creating these "robot users" requires high-level administrative access to the "security office" (App Registrations), which I don't have with a student account.

There is another, simpler method that is perfect for this situation and still keeps the connection secure within the Microsoft cloud. I will use a method called mounting with an Account Key.

Instead of creating a new "robot user," you will temporarily use your own powerful key (the Storage Account Access Key) to establish a permanent link (a mount point) between Databricks and your Data Lake.



Dataset Link: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
