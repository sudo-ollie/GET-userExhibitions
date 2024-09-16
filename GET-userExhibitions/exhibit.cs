using Amazon.DynamoDBv2.DataModel;

namespace SaveExhibition
{
    [DynamoDBTable("Exhibitions")]
    public class Exhibit
    {
        [DynamoDBHashKey("PK")]
        public string PrivateKey { get; set; }
        [DynamoDBRangeKey("SK")]
        public string SortKey { get; set; }
        public int ExhibitionID { get; set; }
        public bool ExhibitionPublic { get; set; }
        public string ExhibitionName { get; set; }
        public string ExhibitionImage { get; set; }
        public List<ExhibitItem> ExhibitContent { get; set; }
        public int ExhibitionLength { get; set; }
        public string UserID { get; set; }

        public static Exhibit CreateExhibit(
            int exhibitionID,
            bool exhibitionPublic,
            string exhibitionName,
            string exhibitionImage,
            List<ExhibitItem> exhibitContent,
            int exhibitionLength,
            string userID)
        {
            var exhibit = new Exhibit
            {
                ExhibitionID = exhibitionID,
                ExhibitionPublic = exhibitionPublic,
                ExhibitionName = exhibitionName,
                ExhibitionImage = exhibitionImage,
                ExhibitContent = exhibitContent ?? new List<ExhibitItem>(),
                ExhibitionLength = exhibitionLength,
                UserID = userID
            };
            exhibit.CreateKeys();
            return exhibit;
        }

        private void CreateKeys()
        {
            PrivateKey = $"{UserID}-{ExhibitionID}";
            SortKey = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss");
        }
        public void AddExhibitItem(ExhibitItem newItem)
        {
            ExhibitContent.Add(newItem);
            ExhibitionLength = ExhibitContent.Count;
        }
    }



    public class ExhibitItem
    {
        public string ItemCreditline { get; set; }
        public string ItemDepartment { get; set; }
        public int ItemID { get; set; }
        public string ItemClassification { get; set; }
        public string ItemTechnique { get; set; }
        public string ItemTitle { get; set; }
        public int CreationDate { get; set; }
        public string ArtistBirthplace { get; set; }
        public string ArtistName { get; set; }
        public string ItemObjectLink { get; set; }
        public string ItemCentury { get; set; }


        public static ExhibitItem CreateExhibitItem(
            string itemCreditline,
            string itemDepartment,
            int itemID,
            string itemClassification,
            string itemTechnique,
            string itemTitle,
            int creationDate,
            string artistBirthplace,
            string artistName,
            string itemObjectlink,
            string itemCentury
            )
        {
            var ExhibitItem = new ExhibitItem
            {
                ItemCreditline = itemCreditline,
                ItemDepartment = itemDepartment,
                ItemID = itemID,
                ItemClassification = itemClassification,
                ItemTechnique = itemTechnique,
                ItemTitle = itemTitle,
                CreationDate = creationDate,
                ArtistBirthplace = artistBirthplace,
                ArtistName = artistName,
                ItemObjectLink = itemObjectlink,
                ItemCentury = itemCentury
            };

            return ExhibitItem;
        }
    }



}