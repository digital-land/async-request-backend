# govuk-notify-stub

This module provides the necessary [Wiremock](https://wiremock.org/) mappings to provide a stub implementation of 
[GOV UK Notify](https://www.notifications.service.gov.uk/).

GOV UK Notify is a service for UK government organisations to send emails, text messages and letters to their users.
Emails are free while a charge is applicable for each text messages and letters.

## Usage

The module is used in the govuk-notify-stub service within the parent docker-compose.yml file.  The wiremock
directory is mapped to the /home/wiremock directory on the container.

See the [Wiremock docs](https://wiremock.org/docs/) to learn more as well as guidance on how to edit stub behaviour 
or add more stubs.
