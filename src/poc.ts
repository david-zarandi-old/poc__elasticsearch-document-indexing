import { createHash } from "node:crypto";
import { Client } from "@elastic/elasticsearch";

type IndexedParagraph = {
  documentId: string;
  paragraphHash: string;
  paragraph: string;
};

function getContentHash(content: string) {
  return createHash("sha256").update(content).digest("hex");
}

async function runPOC() {
  const uri = process.env.ELASTICSEARCH_CONNECTION_URI!;
  const username = process.env.ELASTICSEARCH_USERNAME!;
  const password = process.env.ELASTICSEARCH_PASSWORD!;
  const client = new Client({
    node: uri,
    auth: {
      username,
      password,
    },
  });

  const INDEX = "document";
  const DOCUMENT_ID = "655cf44b804766e86eae8c56";

  await Promise.all(
    ["Paragraph 1", "Paragraph 2", "Paragraph 3", "Paragraph 4"].map(
      async (paragraph) => {
        await client.index({
          index: INDEX,
          document: {
            documentId: DOCUMENT_ID,
            paragraphHash: getContentHash(paragraph),
            paragraph,
          },
        });
      },
    ),
  );

  await client.indices.refresh({ index: INDEX });

  console.log("Initial paragraphs");
  console.dir(
    (
      await client.search({
        index: INDEX,
        query: {
          match: { documentId: DOCUMENT_ID },
        },
      })
    ).hits.hits,
  );

  const oldParagraphHashes = (
    await client.search({
      index: INDEX,
      query: {
        match: { documentId: DOCUMENT_ID },
      },
    })
  ).hits.hits.map((hit) => (hit._source as IndexedParagraph).paragraphHash);

  const newParagraphs = [
    "Paragraph 1",
    "Paragraph 3",
    "Paragraph 2",
    "Paragraph 5",
  ];
  const newParagraphHashes = newParagraphs.map(getContentHash);

  const oldParagraphsToDelete = oldParagraphHashes.filter(
    (paragraphHash) => !newParagraphHashes.includes(paragraphHash),
  );

  // Delete old paragraphs
  for (const oldParagraphHash of oldParagraphsToDelete) {
    await client.deleteByQuery({
      index: INDEX,
      query: {
        bool: {
          must: {
            match: {
              paragraphHash: oldParagraphHash,
            },
          },
          filter: {
            match: { documentId: DOCUMENT_ID },
          },
        },
      },
    });
  }

  await client.indices.refresh({ index: INDEX });

  console.log("Paragraphs after deletion");
  console.dir(
    (
      await client.search({
        index: INDEX,
        query: {
          match: { documentId: DOCUMENT_ID },
        },
      })
    ).hits.hits,
  );

  const newParagraphsToIndex = newParagraphs.filter(
    (_paragraph, index) =>
      !oldParagraphHashes.includes(newParagraphHashes[index]),
  );

  // Add new paragraphs
  for (const newParagraph of newParagraphsToIndex) {
    await client.index({
      index: INDEX,
      document: {
        documentId: DOCUMENT_ID,
        paragraphHash: getContentHash(newParagraph),
        paragraph: newParagraph,
      },
    });
  }

  await client.indices.refresh({ index: INDEX });

  console.log("Paragraphs after addition");
  console.dir(
    (
      await client.search({
        index: INDEX,
        query: {
          match: { documentId: DOCUMENT_ID },
        },
      })
    ).hits.hits,
  );

  await client.indices.delete({ index: INDEX });
  await client.close();
}

runPOC().catch(console.dir);
