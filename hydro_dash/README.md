# hydro_dash

Based on Bradley Erickson's excellent example template for Dash applications: [dash-app-structure](https://github.com/bradley-erickson/dash-app-structure)
- Adds Docker Compose for dev and production; fixes a gunicorn issue from the original repo and migrates setup away from venv to pure Docker by allowing project resources to be mounted into the dev container and copied into production container for deployment.

## prerequisites
- Docker
- Docker compose

## Install and Setup, for dev

```bash
docker-compose build
docker-compose up
```

## Structure

The following is an overview of the structure.
I will follow the structure from top down, covering each item and its purpose.
Additionally, I've created this repository to demonstrate the structure and serve as a template for anyone who wants to fork it.
Each file in the repository includes more information about the purpose of the file.

```bash
hydro_dash
|-- requirements.txt
|-- .env
|-- .gitignore
|-- License
|-- README.md
|-- src
|   |-- assets
|   |   |-- logos/
|   |   |-- css/
|   |   |-- images/
|   |   |-- scripts/
|   |   |-- favicon.ico
|   |-- components
|   |   |-- __init__.py
|   |   |-- footer.py
|   |   |-- navbar.py
|   |   |-- component1.py
|   |-- pages
|   |   |-- __init__.py
|   |   |-- complex_page
|   |   |   |-- __init__.py
|   |   |   |-- layout.py
|   |   |   |-- page_specific_component.py
|   |   |-- home.py
|   |   |-- not_found_404.py
|   |-- utils
|   |   |-- __init__.py
|   |   |-- common_functions.py
|   |-- app.py
```

### Environment Variables

The `.env` file is where you should house any passwords or keys.
This is a common practice as we do not want to directly hardcode keys into your application where a malicious actor could see them.
Some common values found in `.env` files are `DATABASE_URI` or `API_KEY`.
Later on in this guide, we will see how the data is loaded.

This may be stored in the .env file or in the environment variables within the docker-compose.yml and production.yml files.

### src

The `src` directory is where to house all of the code.
We will dive into each item below.

#### Assets

The assets directory contains css files, javascript files, locally hosted images, site logos, and the application icon.
These should be organized into folders based on their purpose.

Note: some of the mentioned directories are not incldued in the sample repository as Git ignores empty directories by default.

#### Components

The components directory should contain common components used throughout the application.
The simplest example are that of the navigation bar, `navbar.py`, and footer element, `footer.py`.
All-in-one components should also be stored here.
Defining each of these components in their own file is good practice.
Additionally, I import each component into the `__init__.py` file.
This allows for easier imports like the following:

```python
from components import navbar, footer
```

#### Pages

Large structured applications rely on the new Dash Pages feature, currently only available via Dash Labs.
This guide will be updated once Dash Pages is included in an official Dash release.
For more information, see the community post at [https://community.plotly.com/t/introducing-dash-pages-a-dash-2-x-feature-preview/57775](https://community.plotly.com/t/introducing-dash-pages-a-dash-2-x-feature-preview/57775).

The pages directory houses the individual pages of your application.
I split the pages into 2 categories, static and complex pages.
The static pages are ones that do not have any, or minimal callbacks, such as your home, privacy policy, about, 404 not found, or contact page.
The complex pages are ones that contain more complex layouts and callbacks.

The static pages should be included immediately under the pages directory.
While the complex pages should be included in their own directory inside the pages.

See the files within the `complex_page` directory and the pages forum post for more information about how to structure more complex pages.

#### Utilities

The utilities directory, `utils`, is meant for common funtions run throughout the application.
Splitting these up into specific files allows for more organized code.

In the example repository, there are 2 files, `api.py` and `images.py`.

The `api.py` file reads in our environment variables to get the `API_KEY`.
The sample API called does not require a key; however, I deemed it important to include anyways.
This file also defines a function that formats the inputs to call the API.
To call the API, we just need to import and call the `get_number_fact(int)` method.

The `images.py` file focuses on anything to do with images shown in our application.
Some of the main functionality includes reading in local images and converting them to encoded strings so they show up properly.
Addtionally, if you are displaying images hosted on some Content Distribution Network (CDN), you might also define a method for formatting the url here.

#### App

The `app.py` file is the entrypoint to defining and running the application.
This is where we define the Dash app, set external stylesheets, and run the app.

As it stands, Dash requires the `app` object for defining `long_callbacks`.
Since this is the only place in the codebase that can access the app object, without ciruclar imports, this file should house any `long_callbacks`.
