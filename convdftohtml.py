import pandas as pd

def convertNormal(df):
    """
    PARAMETER
    ---------
    df: dataframe
    RETURNS
    --------
    html: dataframe converted to html
    """
    html_tab=df.to_html(classes='table table-striped',index=False)
    html='''\
        <html>
            <head>
                <style>
                table, th, td {{
                    border: 1px solid black;
                    border-collapse: collapse;
                    }}
                    th, td {{
                        padding: 5px;
                        text-align: left;    
                    }}  
                </style>
            </head>
            <body>
                <br>
                {0}
            </body>
        </html>
        '''.format(html_tab)
    return html


