# UCI-Framelist
Pulling the PDF of the UCI Framelist into a pandas df

A script for:
  1. reading in the PDF of the UCI framelist using PyPDF2 and Tabula
  2. cleaning the resulting df (a bit, at least)
  3. If the list was updated, posting it to a google sheet (which I keep embedded at https://syllepsis.live/uci-framelist/ )

The Google Sheet, in Google Drive, can be subscribed to for email notifications when the list is updated.
