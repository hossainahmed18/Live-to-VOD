import { S3Client, ListObjectsV2Command, PutObjectCommand } from "@aws-sdk/client-s3";
import { readFileSync } from 'fs';
import { promises as fs } from 'fs';

const getArgumentValue = (argName) => {
    const index = process.argv.indexOf(`--${argName}`);
    return (index > -1) ? process.argv[index + 1] : null;
};

const s3Client = new S3Client({ region: getArgumentValue("region") });

const filterS3FilesByTimeRange = async () => {
    let filteredFiles = [];
    let continuationToken;

    const start_time_param = getArgumentValue("start_time");
    const end_time_param = getArgumentValue("end_time");

    const startTime = new Date(start_time_param);
    const endTime = new Date(end_time_param);

    const bucketName = getArgumentValue("bucket");
    const folderName = getArgumentValue("folder");

    do {
        const params = {
            Bucket: bucketName,
            Prefix: folderName,
            ContinuationToken: continuationToken
        };

        const command = new ListObjectsV2Command(params);
        const response = await s3Client.send(command);

        const filtered = response.Contents?.filter((obj) => {
            const lastModified = new Date(obj.LastModified);
            return lastModified >= startTime && lastModified <= endTime && obj.Key.endsWith(".ts");
        }) || [];

        filteredFiles = [...filteredFiles, ...filtered.map(obj => obj.Key)];
        continuationToken = response.IsTruncated ? response.NextContinuationToken : null;
    } while (continuationToken);

    const fileNames = filteredFiles.map((key) => key.split("/").pop());

    const hlsContent = 
`#EXTM3U
#EXT-X-VERSION:4
#EXT-X-TARGETDURATION:4
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-PLAYLIST-TYPE:VOD
${fileNames.map((file) => `#EXTINF:4.000000,\n${file}`).join("\n")}
#EXT-X-ENDLIST
`;
    const vodHlsFileName = `${folderName}-live-to-vod-${start_time_param}-${end_time_param}.m3u8`;
    await fs.writeFile(vodHlsFileName, hlsContent);
    const convertedFile = readFileSync(vodHlsFileName);

    const putObjectParam = {
        Bucket: bucketName,
        Key: `${folderName}/${vodHlsFileName}`,
        Body: convertedFile
    };
    await s3Client.send(new PutObjectCommand(putObjectParam));

};


(async () => {
    try {
        await filterS3FilesByTimeRange();
    } catch (err) {
        console.error("Error filtering files:", err);
    }
})();


//node index.js --region eu-north-1 --bucket eu-north-1-dev-video-live-vimond-live-output --folder f2d235f9-baa0-4d82-82e0-4cab424813cb --start_time "2024-10-29T09:07:00Z" --end_time "2024-10-29T09:11:00Z"
