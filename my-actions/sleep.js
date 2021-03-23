function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main(args) {
  let ms = args.ms || 1000;
  let message = "Sleeping " + ms + "ms.";
  console.log(message);
  await sleep(ms);
  return { body: message };
}
