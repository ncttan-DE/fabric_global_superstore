[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"departments csv" = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navigation 1" = Source{[workspaceId = "14bc441d-af57-4449-94e9-848ed4994395"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "0e584ec2-ac1b-4527-a56a-135ad6afbedf"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[Name = "raw"]}[Content],
  #"Navigation 5" = #"Navigation 4"{[Name = "departments.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 5", [Delimiter = ",", Columns = 4, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"department_id", Int64.Type}, {"department_name", type text}, {"manager", type text}, {"location", type text}})
in
  #"Changed column type";
shared #"employees csv" = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navigation 1" = Source{[workspaceId = "14bc441d-af57-4449-94e9-848ed4994395"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "0e584ec2-ac1b-4527-a56a-135ad6afbedf"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[Name = "raw"]}[Content],
  #"Navigation 5" = #"Navigation 4"{[Name = "employees.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 5", [Delimiter = ",", Columns = 5, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"employee_id", Int64.Type}, {"name", type text}, {"department_id", Int64.Type}, {"hire_date", type date}, {"salary", Int64.Type}})
in
  #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Merge_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Merge = let
  Source = Table.NestedJoin(#"departments csv", {"department_id"}, #"employees csv", {"department_id"}, "employees csv", JoinKind.LeftOuter),
  #"Expanded employees csv" = Table.ExpandTableColumn(Source, "employees csv", {"employee_id", "name", "department_id", "hire_date", "salary"}, {"employee_id", "name", "department_id.1", "hire_date", "salary"}),
  #"Split column by delimiter" = Table.SplitColumn(#"Expanded employees csv", "name", Splitter.SplitTextByDelimiter(" "), {"name.1", "name.2"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Split column by delimiter", {{"name.1", type text}, {"name.2", type text}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"name.1", "last_name"}, {"name.2", "first_name"}}),
  #"Choose columns" = Table.SelectColumns(#"Renamed columns", {"department_id", "department_name", "manager", "location", "last_name", "first_name", "hire_date", "salary"})
in
  #"Choose columns";
shared DefaultDestination = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "14bc441d-af57-4449-94e9-848ed4994395"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "0e584ec2-ac1b-4527-a56a-135ad6afbedf"]}[Data]
in
  #"Navigation 2";
shared Merge_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "14bc441d-af57-4449-94e9-848ed4994395"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "0e584ec2-ac1b-4527-a56a-135ad6afbedf"]}[Data],
  TableNavigation = Navigation_2{[Id = "department_employees", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
