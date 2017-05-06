'CLARK is a butt and is bad at playing catch

'create system object
Set objFS = CreateObject("Scripting.FileSystemObject")

'specify location of a text file with file names in target folder; each file name is on a new line
strFile = "C:\Users\SCIP2\Documents\Demand Forecasting III\Templates\filenames.txt"

'create object for text file with file names
Set objFile = objFS.OpenTextFile(strFile)
 
'loop through the file names
Do Until objFile.AtEndOfStream

	'assign variable for each file name
    strLine = objFile.ReadLine
	filePath = strLine


	'open file with excel and click refresh
  		Dim xlApp
  		Dim xlBook
  		Dim xlSheet

  		Set xlApp = CreateObject("Excel.Application")
  		Set xlBook = xlApp.Workbooks.Open(filePath, 0, False)


  		xlApp.DisplayAlerts = False
  		xlApp.Visible = False

  		xlBook.RefreshAll

  		xlBook.Save
  		xlBook.Close
  		xlApp.Quit



Loop

objFile.Close

