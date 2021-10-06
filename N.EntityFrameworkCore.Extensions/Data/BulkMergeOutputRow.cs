namespace N.EntityFrameworkCore.Extensions
{
    public class BulkMergeOutputRow<T>
    {
        public string Action { get; set; }
        public string Id { get; set; }

        public BulkMergeOutputRow(string action, string id)
        {
            this.Action = action;
            this.Id = id;
        }
    }
}