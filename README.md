# Scam message classifier

## Introduction
The goal of this project is to classify outbound communication (email, text message) as either 'Scam-risk' or 'Safe'.

We use training data from serveral sources:
- 4,353 sample bad emails from Honeypot (https://github.com/rf-peixoto/phishing_pot)
- 7,328 sample bad emails from Kaggle (https://www.kaggle.com/datasets/subhajournal/phishingemails)
- 16 sample bad synthetic emails from Gemini Pro
- 1 bad email from KWRI (ETH example from Jake)
- 6,133 good emails from KWRI
- Future iterations may include knowing Phishing attempts stored in KWRI's Zendesk instance

We preprocess sample data that may contain plain text, HTLM, and/or email formatts:
- HTML sanitizer
	* Remove unwanted elements and their content: style, script, links, images
    * Remove empty tags
    * Remove unnecessary attributes from allowed tags (src, hfref, alt, title, name, id, class)
- Email
    * Keep BODY and SUBJECT
- Text scrubbing
    * Unidecode
	* Whitespace

Our Model blueprint
- Generate text variables (ex: N-gams)
- Matrix of word-gram occurrences using tfidf
- Slim Residual Neural Network Classifier (but this method is almost indistinguishable in performance from other methods like Elastic-Net classifier, Naïve Bayes, etc.)

Initial performance on Cross-validation & Holdout data
- Sensitivity (Recall) - 0.9985
- Precision (PPV) - 0.9981
- Accuracy - 0.9971
- F1 - 0.9983

Needs exposure to real KWRI volumes for better out-of-sample testing.  Therefore anticipate need for additional training data & tuning

The model is exposed as an API deployed on DataRobot SaaS and may be accssed via webrequests and/or curl.

    ```
	curl -X POST "https://keller-williams-realty.orm.datarobot.com/predApi/v1.0/deployments/*****/predictions"
		 -H "Authorization: Bearer *****"
		 -H "DataRobot-Key: *****"
		 -H "Content-Type: text/csv; charset=UTF-8"
		 -H "Accept: text/csv"
		 --data-binary "@C:\Users\ryan.frederick\Desktop\Book1.csv"

	curl -X POST "https://keller-williams-realty.orm.datarobot.com/predApi/v1.0/deployments/*****/predictions"
		 -H "Authorization: Bearer *****"
		 -H "DataRobot-Key: *****
		 -H "Content-Type: application/json; charset=UTF-8"
		 -H "Accept: text/csv"
		 -d '[{"Email_ID": "123456789", "Email_Content": "Request a Refund. You have a refund of 0.25 ETH (approx. $420.00) available for transaction CPHL5HEFGMTC3YXFUYOIE1LONC."},
			  {"Email_ID": "999999999", "Email_Content": "Property Update: 12345 Cartwright Blvd.  Dear Walter,\n\nJust wanted to give you an update on your property at 12345 Cartwright Blvd. We've received two offers.\n\nCall me at 867-5309 if you have any questions.\n\nSincerely,\nMark King."}]'
    ```


## Installation

### Prerequisites
- Poetry installed on your system (preferably pyenv + poetry)
- Python ^3.12 installed on your system.

### Steps
1. Clone the repository to your local machine.
    ```bash
    git clone <repository_url>
    ```
2. Navigate to the cloned repository.
    ```bash
    cd <repository_name>
    ```
3. Install the the project and all dependencies (outlined in pyproject.toml)
    ```bash
    poetry install
    ```

## Usage

### Relevant Scripts
1. ./Email-HTML-sanitizer.py

2. ./Call-Inferencing-API.py

3. ./Kaggle-to-csv.py

4. ./Synthetic-data-generation.py

## Conclusion
With this guide you will understand the HTML sanitizer and methods to build training data for this Machine Leanring (ML) project.
