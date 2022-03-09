![banner](https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/banner.png)
# Waze CCP on GCP
This is not an officially supported Google product, though support will be provided on a best-effort basis.

Copyright 2018 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

### Introduction

This AppEngine sample application is designed to process your Waze CCP JSON Feed into; BigQuery GIS  tables for analysis, Google Cloud Storage as GeoJSON for use in desktop or web GIS applications,  and, optionally into [Carto](https://carto.com/) for advanced spatial visualization. 

Join the [Group](https://groups.google.com/d/forum/waze-ccp-gcp) for updates and discussion

##### Google Cloud Products Used:
- Google AppEngine
- Google Cloud Datastore
- Google BigQuery
- Google Cloud Storage

### Getting Started

##### Step 1: Create and Configure your Project


<p align="center">
  <img src="https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/1.png" width="600px"/>
</p>

From here on out, we'll refer to your Project ID as **{project-id}**

##### Step 2: Configure BigQuery

###### 1. Enable the BigQuery API

<p align="center">
  <img src="https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/2.png" width="600px"/>
</p>

###### 2. Go to BigQuery UI and Create a Dataset

<p align="center">
  <img src="https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/3.png" width="600px"/>
</p>

From here on out, we'll refer to your Dataset as **{bqDataset}**

<p align="center">
  <img src="https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/4.png" width="600px"/>
</p>

##### Step 3. Go to Cloud Storage UI and Create a Bucket

<p align="center">
  <img src="https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/5.png" width="600px"/>
</p>

From here on out, we'll refer to your Bucket as **{gcsBucket}**

##### Step 4. Download source code, Install Dependencies, and Update Variables to Match your Project.
###### 1. Clone This Source Code: 
```git clone https://github.com/google/waze-ccp-gcp.git ```
###### 2. Update Variables Source Code: 
First, generate a [GUID](https://www.guidgenerator.com/). This will be referred to as **{guid}** and its just a way to create a non-guessable URL for the handler that Cron will call to update the Waze data every 10 minutes. 

- In cron.yaml 
  - Line 17: Change **{guid}** to your **{guid}**
- In main.py
  - Line 32: Change **{waze-url}** to your Waze CCP URL
  - Line 40: Change **{gcsBucket}** to your **{gcsBucket}** 
  - Line 46: Change **{bqDataset}** to your **{bqDataset}**
  -  Line 336: Change **{guid}** to your **{guid}**

###### 3. Check dependencies: 
This application utilizes various Python libraries, and App Engine will use the requirements.txt file during the build. You are free to check the versions and update accordingly. 

From the terminal, change directories to where you cloned the source code. 
``` cd {your-app-folder} ```


##### Step 5. Deploy your Application to AppEngine


###### 1. Using gcloud, Switch Project to your New Project:
``` gcloud config set project {project-id} ```
###### 2. Using gcloud, Switch Project to your New Project:
``` gcloud app create ```

<p align="center">
  <img src="https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/9.png" width="600px"/>
</p>

For the next step, you'll need to go to the [Cloud Build settings page](https://console.cloud.google.com/cloud-build/settings/service-account). 

<p align="center">
  <img src="https://github.com/WandLZhang/waze-ccp-gcp/blob/master/img/waze-ccp-1.png?raw=true" width="600px"/>
</p>

This will trigger a second prompt to enable the App Engine Admin API.

<p align="center">
  <img src="https://github.com/WandLZhang/waze-ccp-gcp/blob/master/img/waze-ccp-2.png?raw=true" width="600px"/>
</p>

###### 3. Using gcloud, Deploy your Application and Create a Case:

Deploy the main application:
```gcloud beta app deploy {your-app-folder}/app.yaml```

Then start the cron job:
```gcloud app deploy {your-app-folder}/cron.yaml```

###### 4. Secure your Application with Identity Aware Proxy:
Even though you generated a GUID to serve as the URL path that AppEngine's Cron accesses to cause a data update, someone could discover it and maliciously hit that URL, and, they could also hit the /newCase/ endpoint. In order to prevent unwanted use of these URLs, you will enable IAP and lock down access to the application only to approved users (or just you). 

When you go to IAP settings for your project, you'll first have to set up a Credentials Screen (Oath2). 
Set the Application Type to "Internal".

<p align="center">
  <img src="https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/7.png" width="600px"/>
</p>

Then, under IAP - turn the IAP on for the AppEngine app: 

<p align="center">
  <img src="https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/8.png" width="600px"/>
</p>

You can verify that IAP is working by visiting https://{project-id}.appspot.com in an Incognito browser. You should be redircted to your OAuth2 Credentials Screen, which shows that the IAP is working and protecting the entire application. 

##### Step 6. Create your New Case Study

Visit https://{project-id}.appspot.com/newCase/{case-name} to initiate a case, where {case-name} is any name you create for your case (like "Miami" in the example screenshot below). Be careful to just do this once per case you want to create.

To confirm the Case Study was created, you can visit Datastore and confirm the Entity you expect to see is there. 
<p align="center">
  <img src="https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/10.png" width="8600px"/>
</p>

The Cron job described in cron.yaml will call https://{project-id}.appspot.com/{guid}/ and will start populating the tables in BigQuery. *Note - the cron function of AppEngine is internal so it is automatically inscope for IAP purposes. 

##### Step 7. Investigating the Waze Data
###### BigQuery:
In BigQuery, you should see the three tables (alerts, jams, irregularities) under your **{bqDataset}**
<p align="center">
  <img src="https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/11.png" width="600px"/>
</p>

These tables will contain all the **unique** elements from your **{waze-url}**
<p align="center">
  <img src="https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/12.png" width="800px"/>
</p>

###### Data Studio:
Once you have a few days worth of data, you can start experimenting with building Data Studio dashboards.
<p align="center">
  <img src="https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/6.png" width="800px"/>
</p>If you come up with something interesting, be sure to share with the group: <waze-ccp-on-gcp@googlegroups.com>
